"""
Load Stack Exchange data from Parquet into Neo4j.
Handles the unified schema where all node types are in one DataFrame.

Usage:
    spark-submit \\
      --packages org.neo4j:neo4j-connector-apache-spark_2.12:5.3.1_for_spark_3 \\
      --driver-memory 4g \\
      --executor-memory 4g \\
      python/graphframes/tutorials/neo4j_load.py
"""
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame

# Get the directory containing this script (works regardless of where you run from)
# Note: This file is in neo4j/, so we go up one level to find data/
SCRIPT_DIR = Path(__file__).parent.resolve()

# Initialize SparkSession with Neo4j connector
spark = (
    SparkSession.builder
    .appName("Load Stack Exchange into Neo4j")
    .config("spark.jars.packages", "org.neo4j:neo4j-connector-apache-spark_2.12:5.3.1_for_spark_3")
    .config("neo4j.url", "neo4j://localhost:7687")
    .config("neo4j.authentication.basic.username", "neo4j")
    .config("neo4j.authentication.basic.password", "graphframes123")
    .config("neo4j.database", "neo4j")
    .getOrCreate()
)

# Load the GraphFrames data
STACKEXCHANGE_SITE = "stats.meta.stackexchange.com"
BASE_PATH = str(SCRIPT_DIR.parent / "data" / STACKEXCHANGE_SITE)

print("=" * 80)
print("LOADING STACK EXCHANGE DATA INTO NEO4J")
print("=" * 80)

print("\n[1/4] Loading nodes from Parquet...")
nodes_df: DataFrame = spark.read.parquet(f"{BASE_PATH}/Nodes.parquet")
total_nodes = nodes_df.count()
print(f"✓ Loaded {total_nodes:,} nodes")

print("\n[2/4] Loading edges from Parquet...")
edges_df: DataFrame = spark.read.parquet(f"{BASE_PATH}/Edges.parquet")
total_edges = edges_df.count()
print(f"✓ Loaded {total_edges:,} edges")

# Show data summary
print("\nNode types:")
nodes_df.groupBy("Type").count().orderBy(F.desc("count")).show()

print("\nEdge types:")
edges_df.groupBy("relationship").count().orderBy(F.desc("count")).show()

# Write nodes to Neo4j grouped by type
print("\n[3/4] Writing nodes to Neo4j...")
node_types = [row["Type"] for row in nodes_df.select("Type").distinct().collect()]

for node_type in node_types:
    print(f"\n  Loading {node_type} nodes...")

    # Filter nodes of this type
    type_nodes = nodes_df.filter(F.col("Type") == node_type)
    count = type_nodes.count()
    print(f"    Found {count:,} nodes")

    # Write to Neo4j with Type as label
    # Use 'id' as the node key to ensure uniqueness
    (
        type_nodes.write
        .format("org.neo4j.spark.DataSource")
        .mode("Overwrite" if node_type == node_types[0] else "Append")
        .option("labels", f":{node_type}")
        .option("node.keys", "id")
        .save()
    )
    print(f"    ✓ Loaded {count:,} {node_type} nodes")

# Write relationships to Neo4j
print("\n[4/4] Writing relationships to Neo4j...")
relationship_types = [row["relationship"] for row in edges_df.select("relationship").distinct().collect()]

for rel_type in relationship_types:
    print(f"\n  Loading {rel_type} relationships...")

    # Filter edges of this type
    type_edges = edges_df.filter(F.col("relationship") == rel_type)
    count = type_edges.count()
    print(f"    Found {count:,} relationships")

    # Prepare edges for Neo4j connector format
    edges_for_neo4j = (
        type_edges
        .select(
            F.col("src").alias("source.id"),
            F.col("dst").alias("target.id"),
            F.col("relationship")
        )
    )

    # Write relationships to Neo4j
    (
        edges_for_neo4j.write
        .format("org.neo4j.spark.DataSource")
        .mode("Append")
        .option("relationship", rel_type)
        .option("relationship.save.strategy", "keys")
        .option("relationship.source.labels", ":Node")
        .option("relationship.source.node.keys", "source.id:id")
        .option("relationship.target.labels", ":Node")
        .option("relationship.target.node.keys", "target.id:id")
        .save()
    )
    print(f"    ✓ Loaded {count:,} {rel_type} relationships")

print("\n" + "=" * 80)
print("✓ DATA LOAD COMPLETE!")
print("=" * 80)
print(f"\nLoaded:")
print(f"  • {total_nodes:,} nodes across {len(node_types)} types")
print(f"  • {total_edges:,} relationships across {len(relationship_types)} types")
print("\nNext steps:")
print("  1. Verify in Neo4j Browser: http://localhost:7474 (neo4j / graphframes123)")
print("  2. Run: MATCH (n) RETURN labels(n)[0] as Type, count(*) ORDER BY count(*) DESC")
print("  3. Run connected components: python/graphframes/tutorials/neo4j/connected_components.py")

spark.stop()
