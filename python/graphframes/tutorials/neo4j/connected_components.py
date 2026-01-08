"""
Read Stack Exchange data from Neo4j, run Connected Components, write results back.

Usage:
    spark-submit \\
      --packages org.neo4j:neo4j-connector-apache-spark_2.12:5.3.1_for_spark_3 \\
      --driver-memory 4g \\
      --executor-memory 4g \\
      python/graphframes/tutorials/neo4j_connected_components.py
"""
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from graphframes import GraphFrame

print("=" * 80)
print("NEO4J ↔ GRAPHFRAMES: CONNECTED COMPONENTS")
print("=" * 80)

# Initialize SparkSession with Neo4j connector
print("\n[1/6] Initializing Spark with Neo4j connector...")
spark = (
    SparkSession.builder
    .appName("Neo4j GraphFrames Connected Components")
    .config("spark.jars.packages", "org.neo4j:neo4j-connector-apache-spark_2.12:5.3.1_for_spark_3")
    .config("neo4j.url", "neo4j://localhost:7687")
    .config("neo4j.authentication.basic.username", "neo4j")
    .config("neo4j.authentication.basic.password", "graphframes123")
    .config("neo4j.database", "neo4j")
    .config("spark.sql.caseSensitive", True)
    .getOrCreate()
)

# Set checkpoint directory for Connected Components
spark.sparkContext.setCheckpointDir("/tmp/graphframes-neo4j-checkpoints")
print("✓ Spark session initialized")

# Read all nodes from Neo4j
print("\n[2/6] Reading nodes from Neo4j...")
vertices_df = (
    spark.read
    .format("org.neo4j.spark.DataSource")
    .option("query", """
        MATCH (n)
        RETURN
            n.id as id,
            labels(n)[0] as Type,
            n.Id as Id,
            n.Name as Name,
            n.DisplayName as DisplayName,
            n.Title as Title,
            n.Score as Score,
            n.CreationDate as CreationDate
    """)
    .load()
)

node_count = vertices_df.count()
print(f"✓ Read {node_count:,} nodes from Neo4j")

print("\nNode type distribution:")
vertices_df.groupBy("Type").count().orderBy(F.desc("count")).show()

# Read all relationships from Neo4j
print("\n[3/6] Reading relationships from Neo4j...")
edges_df = (
    spark.read
    .format("org.neo4j.spark.DataSource")
    .option("query", """
        MATCH (source)-[r]->(target)
        RETURN
            source.id as src,
            target.id as dst,
            type(r) as relationship
    """)
    .load()
)

edge_count = edges_df.count()
print(f"✓ Read {edge_count:,} relationships from Neo4j")

print("\nRelationship type distribution:")
edges_df.groupBy("relationship").count().orderBy(F.desc("count")).show()

# Create GraphFrame
print("\n[4/6] Creating GraphFrame...")
g = GraphFrame(vertices_df, edges_df)
print(f"✓ GraphFrame created")
print(f"  Vertices: {g.vertices.count():,}")
print(f"  Edges: {g.edges.count():,}")

# Run Connected Components
print("\n[5/6] Running Connected Components algorithm...")
print("This finds all disconnected subgraphs in the network.")
print("Note: This may take several minutes on large graphs...")
print()

# Run the algorithm
result = g.connectedComponents()

# Analyze the components
print("✓ Connected Components complete!")

print("\n" + "-" * 80)
print("COMPONENT ANALYSIS")
print("-" * 80)

# Calculate component statistics
component_stats = (
    result
    .groupBy("component")
    .agg(
        F.count("*").alias("size"),
        F.collect_set("Type").alias("node_types")
    )
    .orderBy(F.desc("size"))
)

print("\nTop 10 largest components:")
component_stats.show(10, truncate=False)

# Show total number of components
total_components = component_stats.count()
print(f"\nTotal components found: {total_components:,}")

# Show component size distribution
print("\nComponent size distribution:")
size_distribution = (
    component_stats
    .groupBy("size")
    .count()
    .orderBy("size")
    .withColumnRenamed("count", "num_components")
)
size_distribution.show(20)

# Analyze the largest component
largest_component = component_stats.first()
print(f"\nLargest component:")
print(f"  ID: {largest_component['component']}")
print(f"  Size: {largest_component['size']:,} nodes")
print(f"  Node types: {largest_component['node_types']}")

# Show sample nodes from largest component
print("\nSample nodes from largest component:")
(
    result
    .filter(F.col("component") == largest_component["component"])
    .select("id", "Type", "Name", "Title", "DisplayName")
    .show(10, truncate=True)
)

# Write component IDs back to Neo4j
print("\n[6/6] Writing component IDs back to Neo4j...")

# Select just id and component for writing back
component_results = result.select("id", "component")

# Write back to Neo4j by updating nodes with component property
print("Updating nodes with component IDs...")
(
    component_results.write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("query", """
        UNWIND $rows AS row
        MATCH (n {id: row.id})
        SET n.component = row.component
        RETURN count(n) as updated
    """)
    .save()
)

print(f"✓ Updated {component_results.count():,} nodes with component IDs")

# Verify the update in Neo4j
print("\nVerifying updates in Neo4j...")
verification = (
    spark.read
    .format("org.neo4j.spark.DataSource")
    .option("query", """
        MATCH (n)
        WHERE n.component IS NOT NULL
        RETURN count(n) as nodesWithComponent
    """)
    .load()
)

nodes_with_components = verification.first()["nodesWithComponent"]
print(f"✓ Verified: {nodes_with_components:,} nodes now have component IDs")

print("\n" + "=" * 80)
print("✓ PIPELINE COMPLETE!")
print("=" * 80)

print("\nResults summary:")
print(f"  • Analyzed {node_count:,} nodes and {edge_count:,} relationships")
print(f"  • Found {total_components:,} connected components")
print(f"  • Largest component: {largest_component['size']:,} nodes")
print(f"  • Updated {nodes_with_components:,} nodes in Neo4j")

print("\nNext steps:")
print("  1. Open Neo4j Browser: http://localhost:7474")
print("  2. View components: MATCH (n) WHERE n.component IS NOT NULL")
print("                      RETURN n.component, count(*) ORDER BY count(*) DESC")
print("  3. Visualize a component: MATCH (n {component: <id>})-[r]-(m)")
print("                             RETURN n,r,m LIMIT 100")
print("  4. Find Questions in same component:")
print("     MATCH (q1:Question)-[*1..3]-(q2:Question)")
print("     WHERE q1.component = q2.component AND id(q1) < id(q2)")
print("     RETURN q1.Title, q2.Title, q1.component LIMIT 10")

spark.stop()
