"""
Test script for Neo4j integration tutorial.
Tests the data processing logic without requiring Neo4j.
"""
import pyspark.sql.functions as F
from graphframes import GraphFrame
from pyspark.sql import SparkSession, DataFrame

print("=" * 80)
print("NEO4J TUTORIAL - CODE VALIDATION")
print("=" * 80)

# Initialize SparkSession (without Neo4j connector for testing)
print("\n[1/5] Initializing Spark...")
spark = (
    SparkSession.builder
    .appName("Neo4j Tutorial Validation")
    .config("spark.sql.caseSensitive", True)
    .getOrCreate()
)
spark.sparkContext.setCheckpointDir("/tmp/graphframes-test-checkpoints")
print("✓ Spark initialized")

# Load the GraphFrames data
STACKEXCHANGE_SITE = "stats.meta.stackexchange.com"
BASE_PATH = f"python/graphframes/tutorials/data/{STACKEXCHANGE_SITE}"

print(f"\n[2/5] Loading Stack Exchange data from {BASE_PATH}...")
try:
    nodes_df: DataFrame = spark.read.parquet(f"{BASE_PATH}/Nodes.parquet")
    edges_df: DataFrame = spark.read.parquet(f"{BASE_PATH}/Edges.parquet")
    print("✓ Data loaded successfully")
except Exception as e:
    print(f"✗ Failed to load data: {e}")
    print("\nPlease run the data preparation first:")
    print("  graphframes stackexchange stats.meta")
    print("  spark-submit --packages com.databricks:spark-xml_2.12:0.18.0 \\")
    print("    --driver-memory 4g --executor-memory 4g \\")
    print("    python/graphframes/tutorials/stackexchange.py")
    spark.stop()
    exit(1)

# Validate data structure
print("\n[3/5] Validating data structure...")

node_count = nodes_df.count()
edge_count = edges_df.count()
print(f"  Nodes: {node_count:,}")
print(f"  Edges: {edge_count:,}")

# Check for required columns
required_node_cols = ["id", "Type"]
required_edge_cols = ["src", "dst", "relationship"]

node_cols = set(nodes_df.columns)
edge_cols = set(edges_df.columns)

assert all(col in node_cols for col in required_node_cols), f"Missing node columns: {set(required_node_cols) - node_cols}"
assert all(col in edge_cols for col in required_edge_cols), f"Missing edge columns: {set(required_edge_cols) - edge_cols}"
print("✓ Data structure validated")

# Show node and edge types (as they would appear in Neo4j)
print("\nNode types (would become Neo4j labels):")
node_types_df = nodes_df.groupBy("Type").count().orderBy(F.desc("count"))
node_types_df.show()

print("\nRelationship types (would become Neo4j relationship types):")
edge_types_df = edges_df.groupBy("relationship").count().orderBy(F.desc("count"))
edge_types_df.show()

# Simulate the Neo4j query result structure
print("\n[4/5] Simulating Neo4j data transformation...")

# This simulates what we'd get back from Neo4j
# Select only columns that exist (some are type-specific)
select_cols = ["id", "Type"]
optional_cols = ["Name", "DisplayName", "Title", "Score", "CreationDate"]

# Add optional columns if they exist
for col in optional_cols:
    if col in nodes_df.columns:
        select_cols.append(col)

simulated_vertices = nodes_df.select(*select_cols)

simulated_edges = edges_df.select(
    F.col("src"),
    F.col("dst"),
    F.col("relationship")
)

print("✓ Simulated Neo4j query results")
print(f"  Vertices: {simulated_vertices.count():,}")
print(f"  Edges: {simulated_edges.count():,}")

# Create GraphFrame (as we would after reading from Neo4j)
print("\n[5/5] Creating GraphFrame and testing Connected Components...")
g = GraphFrame(simulated_vertices, simulated_edges)
print(f"✓ GraphFrame created")
print(f"  Vertices: {g.vertices.count():,}")
print(f"  Edges: {g.edges.count():,}")

# Run Connected Components (this is the actual algorithm we'd run)
print("\nRunning Connected Components (this may take a minute)...")
result = g.connectedComponents()

# Analyze results
component_stats = (
    result
    .groupBy("component")
    .agg(
        F.count("*").alias("size"),
        F.collect_set("Type").alias("node_types")
    )
    .orderBy(F.desc("size"))
)

total_components = component_stats.count()
largest_component = component_stats.first()

print(f"✓ Connected Components complete!")
print(f"\nResults:")
print(f"  Total components: {total_components:,}")
print(f"  Largest component size: {largest_component['size']:,} nodes")
print(f"  Largest component types: {largest_component['node_types']}")

print("\nTop 5 components by size:")
component_stats.show(5, truncate=False)

print("\nComponent size distribution:")
(
    component_stats
    .groupBy("size")
    .count()
    .orderBy("size")
    .withColumnRenamed("count", "num_components")
    .show(10)
)

# Simulate what would be written back to Neo4j
print("\n" + "-" * 80)
print("Data that would be written back to Neo4j:")
print("-" * 80)

component_results = result.select("id", "component")
print(f"  {component_results.count():,} nodes with component assignments")

# Show sample
print("\nSample nodes with component IDs:")
(
    result
    .select("id", "Type", "component", "Name", "Title")
    .show(10, truncate=True)
)

# Verify data quality
print("\n" + "-" * 80)
print("Data Quality Checks:")
print("-" * 80)

# Check for nulls
null_components = result.filter(F.col("component").isNull()).count()
print(f"  Nodes with null components: {null_components}")
assert null_components == 0, "Found nodes with null component IDs!"

# Check that all original nodes are present
original_count = nodes_df.count()
result_count = result.count()
print(f"  Original nodes: {original_count:,}")
print(f"  Result nodes: {result_count:,}")
assert original_count == result_count, f"Node count mismatch! {original_count} vs {result_count}"

print("\n✓ All data quality checks passed!")

print("\n" + "=" * 80)
print("✓ NEO4J TUTORIAL CODE VALIDATION COMPLETE")
print("=" * 80)

print("\nValidation Summary:")
print(f"  ✓ Data loading works correctly")
print(f"  ✓ Data structure matches expected format")
print(f"  ✓ GraphFrame creation succeeds")
print(f"  ✓ Connected Components algorithm works")
print(f"  ✓ Results have expected structure for Neo4j write-back")
print(f"  ✓ Data quality checks pass")

print("\nTo run the full tutorial with Neo4j:")
print("  1. Start Neo4j: graphframes neo4j start")
print("  2. Load data: graphframes neo4j load --subdomain stats.meta")
print("  3. Run algorithm: spark-submit ... neo4j/connected_components.py")

spark.stop()
