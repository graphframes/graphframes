# Neo4j Integration Tutorial

This tutorial demonstrates how to integrate GraphFrames with Neo4j, enabling you to leverage both graph database persistence and distributed graph analytics to scale expensive algorithms. You'll learn to:

1. Set up Neo4j using Docker
2. Load Stack Exchange data into Neo4j
3. Read graph data from Neo4j into GraphFrames
4. Run distributed graph algorithms (Connected Components)
5. Write enriched results back to Neo4j

By the end, you'll have a complete pipeline for bidirectional data flow between Neo4j and GraphFrames.

## Why Integrate Neo4j with GraphFrames?

Together, they provide a powerful combination: use Neo4j for transactional workloads and GraphFrames for large-scale analytics. **This can decrease your Neo4j bill substantially by offloading heavy computations to Spark.**

**Neo4j** excels at:

- ACID transactions
- Real-time graph queries
- Complex path traversals
- Interactive graph exploration

**GraphFrames** excels at:

- Distributed graph analytics at scale
- Batch processing of billions of edges
- Integration with Spark ML pipelines
- Complex pattern matching with motif finding

## Prerequisites

Before starting, ensure you have:

- **Docker**: For running Neo4j (install from [docker.com](https://www.docker.com/))
- **GraphFrames 0.10.0+**: `pip install graphframes-py`
- **Apache Spark 3.x+**: Compatible with your Python version
- **Stack Exchange Dataset**: Follow the [Network Motif Tutorial](02-motif-tutorial.md) to download

Check your versions:

```bash
docker --version
python -c "import graphframes; print(graphframes.__version__)"
```

## Architecture Overview

Our data pipeline:

```
Stack Exchange Data (Parquet)
         ↓
    [Load into Neo4j]
         ↓
    Neo4j Database ← Docker Container
         ↓
[Read via Neo4j Connector]
         ↓
    PySpark DataFrames
         ↓
    GraphFrames Graph
         ↓
  [Connected Components]
         ↓
    Enriched DataFrames
         ↓
[Write via Neo4j Connector]
         ↓
    Neo4j Database (updated with component IDs)
```

## Step 1: Set Up Neo4j with Docker

First, let's start a Neo4j instance using Docker. We'll use Neo4j Community Edition with APOC plugins for data import capabilities.

Create a directory for Neo4j data:

```bash
mkdir -p /tmp/neo4j-data/data /tmp/neo4j-data/logs /tmp/neo4j-data/import /tmp/neo4j-data/plugins
```

Start Neo4j container:

```bash
docker run -d \
    --name neo4j-graphframes \
    -p 7474:7474 -p 7687:7687 \
    -v /tmp/neo4j-data/data:/data \
    -v /tmp/neo4j-data/logs:/logs \
    -v /tmp/neo4j-data/import:/var/lib/neo4j/import \
    -v /tmp/neo4j-data/plugins:/plugins \
    -e NEO4J_apoc_import_file_enabled=true \
    -e NEO4J_apoc_export_file_enabled=true \
    -e NEO4J_AUTH=neo4j/graphframes123 \
    -e NEO4J_PLUGINS='["apoc"]' \
    -e NEO4J_dbms_security_procedures_unrestricted=apoc.\\\* \
    -e NEO4J_dbms_memory_heap_initial__size=1G \
    -e NEO4J_dbms_memory_heap_max__size=2G \
    neo4j:community
```

**Connection Details:**

- **Browser UI**: <http://localhost:7474>
- **Bolt Protocol**: neo4j://localhost:7687
- **Username**: `neo4j`
- **Password**: `graphframes123`

Verify Neo4j is running:

```bash
docker logs neo4j-graphframes
```

Wait for the message: `Started.`

You can also visit <http://localhost:7474> and log in with the credentials above.

## Step 2: Prepare the Stack Exchange Data

If you haven't already, download and process the Stack Exchange data:

```bash
# Download the Stack Exchange archive
graphframes stackexchange stats.meta

# Spark 3: Process the XML data into Parquet files using spark-xml - https://github.com/databricks/spark-xml
spark-submit --packages com.databricks:spark-xml_2.12:0.18.0 \
  --driver-memory 4g --executor-memory 4g \
  python/graphframes/tutorials/stackexchange.py

# Spark 4: Use native XML support - https://spark.apache.org/docs/4.0.0/sql-data-sources-xml.html
spark-submit \
  --driver-memory 4g --executor-memory 4g \
  python/graphframes/tutorials/stackexchange.py
```

This creates:

```
python/graphframes/tutorials/data/stats.meta.stackexchange.com/
├── Nodes.parquet          # Unified nodes for GraphFrames
├── Edges.parquet          # Unified edges for GraphFrames
├── Users.parquet          # Individual type with UUID ids
├── Questions.parquet      # Individual type with UUID ids
├── Answers.parquet        # Individual type with UUID ids
├── Votes.parquet          # Individual type with UUID ids
├── Tags.parquet           # Individual type with UUID ids
├── Badges.parquet         # Individual type with UUID ids
├── PostLinks.parquet      # Individual type with UUID ids
├── PostHistory.parquet    # Individual type with UUID ids
├── Comments.parquet       # Individual type with UUID ids
└── edges/
    ├── CastFor.parquet    # Vote -> Post relationships
    ├── Asks.parquet       # User -> Question relationships
    ├── Posts.parquet      # User -> Answer relationships
    ├── Answers.parquet    # Answer -> Question relationships
    ├── Tags.parquet       # Tag -> Post relationships
    ├── Earns.parquet      # User -> Badge relationships
    ├── Duplicates.parquet # Post -> Post relationships
    └── Links.parquet      # Post -> Post relationships
```

The individual type parquet files contain UUID `id` fields that match the `src`/`dst` fields in the edge parquet files, enabling loading into Neo4j with proper node labels and relationship types.

## Step 3: Load Stack Exchange Data into Neo4j

The easiest way to load data into Neo4j is using the GraphFrames CLI, which uses APOC to load the individual parquet files with proper labels:

```bash
# Start Neo4j with APOC Extended (for parquet support)
graphframes neo4j start

# Load the data into Neo4j from the individual parquet files
graphframes neo4j load --subdomain stats.meta

# Check the status
graphframes neo4j status
```

This will load all nodes with their proper labels (User, Question, Answer, etc.) and all relationships with their types (ASKS, ANSWERS, EARNS, etc.).

**Alternative: Using the Neo4j Spark Connector to Write to Neo4j**

If you prefer to use the Neo4j Spark Connector for loading, you can use a Python script. This approach handles the multi-type node schema used in the Network Motif Tutorial.

Check out `python/graphframes/tutorials/neo4j/load.py`:

```python
"""
Load Stack Exchange data from Parquet into Neo4j.
Handles the unified schema where all node types are in one DataFrame.
"""
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from typing import List

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
BASE_PATH = f"python/graphframes/tutorials/data/{STACKEXCHANGE_SITE}"

print("Loading nodes from Parquet...")
nodes_df: DataFrame = spark.read.parquet(f"{BASE_PATH}/Nodes.parquet")
print(f"Total nodes: {nodes_df.count():,}")

print("Loading edges from Parquet...")
edges_df: DataFrame = spark.read.parquet(f"{BASE_PATH}/Edges.parquet")
print(f"Total edges: {edges_df.count():,}")

# Check node types
print("\nNode types:")
nodes_df.groupBy("Type").count().orderBy(F.desc("count")).show()

# Check edge types
print("\nEdge types:")
edges_df.groupBy("relationship").count().orderBy(F.desc("count")).show()

# Clear existing data in Neo4j
print("\nClearing existing data in Neo4j...")
spark.sql("""
    CALL {
        MATCH (n) DETACH DELETE n
    }
""").show()

# Load nodes by type
# We'll create nodes with their Type as a label and store all properties
node_types = [row["Type"] for row in nodes_df.select("Type").distinct().collect()]

for node_type in node_types:
    print(f"\nLoading {node_type} nodes...")

    # Filter nodes of this type
    type_nodes = nodes_df.filter(F.col("Type") == node_type)
    count = type_nodes.count()
    print(f"  Found {count:,} {node_type} nodes")

    # Write to Neo4j with Type as label
    # Use 'id' as the node key to ensure uniqueness
    (
        type_nodes.write
        .format("org.neo4j.spark.DataSource")
        .mode("Append")
        .option("labels", f":{node_type}")
        .option("node.keys", "id")
        .save()
    )
    print(f"  ✓ Loaded {count:,} {node_type} nodes")

# Load relationships
print("\nLoading relationships...")
relationship_types = [row["relationship"] for row in edges_df.select("relationship").distinct().collect()]

for rel_type in relationship_types:
    print(f"\n  Loading {rel_type} relationships...")

    # Filter edges of this type
    type_edges = edges_df.filter(F.col("relationship") == rel_type)
    count = type_edges.count()
    print(f"    Found {count:,} {rel_type} relationships")

    # Write relationships to Neo4j
    # Map src->source.id and dst->target.id for Neo4j connector
    (
        type_edges
        .withColumnRenamed("src", "source")
        .withColumnRenamed("dst", "target")
        .write
        .format("org.neo4j.spark.DataSource")
        .mode("Append")
        .option("relationship", rel_type)
        .option("relationship.save.strategy", "keys")
        .option("relationship.source.labels", ":Node")  # Generic label, will match by id
        .option("relationship.source.save.mode", "match")
        .option("relationship.source.node.keys", "source:id")
        .option("relationship.target.labels", ":Node")
        .option("relationship.target.save.mode", "match")
        .option("relationship.target.node.keys", "target:id")
        .save()
    )
    print(f"    ✓ Loaded {count:,} {rel_type} relationships")

print("\n" + "=" * 80)
print("✓ Data load complete!")
print("=" * 80)

# Verify the load
print("\nVerifying data in Neo4j...")
node_count_query = """
CALL {
    MATCH (n)
    RETURN count(n) as nodeCount
}
"""

rel_count_query = """
CALL {
    MATCH ()-[r]->()
    RETURN count(r) as relCount
}
"""

print(f"Nodes in Neo4j: TBD")  # We'll verify this with a Cypher query
print(f"Relationships in Neo4j: TBD")

spark.stop()
print("\n✓ Done!")
```

Run the loading script:

```bash
spark-submit \
  --packages org.neo4j:neo4j-connector-apache-spark_2.12:5.3.1_for_spark_3 \
  --driver-memory 4g \
  --executor-memory 4g \
  python/graphframes/tutorials/neo4j/load.py
```

This will take several minutes depending on your machine.

## Step 4: Verify Data in Neo4j Browser

Open <http://localhost:7474> and run these Cypher queries:

```cypher
// Count all nodes - 164,530
MATCH (n)
RETURN count(n) as totalNodes;

// Count nodes by type
MATCH (n)
RETURN labels(n)[0] as Type, count(n) as count
ORDER BY count DESC;

// Count all relationships - 97,104
MATCH ()-[r]->()
RETURN count(r) as totalRelationships;

// Count relationships by type
MATCH ()-[r]->()
RETURN type(r) as RelType, count(r) as count
ORDER BY count DESC;

// Sample the graph structure
MATCH (a)-[r]->(b)
RETURN a, r, b
LIMIT 25;
```

Expected output:

- Total nodes: ~164,530
- Total relationships: ~97,104
- Node types: Badge, Vote, User, Answer, Question, PostLinks, Tag
- Relationship types: Earns, CastFor, Tags, Answers, Posts, Asks, Links, Duplicates

## Step 5: Read Data from Neo4j into GraphFrames

Now let's read the data back from Neo4j and create a GraphFrame. Check out `python/graphframes/tutorials/neo4j/connected_components.py`:

```python
"""
Read Stack Exchange data from Neo4j, run Connected Components, write results back.
"""
import pyspark.sql.functions as F
from graphframes import GraphFrame
from pyspark.sql import SparkSession, DataFrame

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
edges_df.groupBy("relationship").count().orderBy(F.desc("count")).show()

# Create GraphFrame
print("\n[4/6] Creating GraphFrame...")
g = GraphFrame(vertices_df, edges_df)
print(f"✓ GraphFrame created with {g.vertices.count():,} vertices and {g.edges.count():,} edges")

# Run Connected Components
print("\n[5/6] Running Connected Components algorithm...")
print("This finds all disconnected subgraphs in the network.")
print("Note: This may take several minutes on large graphs...")

# Run the algorithm
result = g.connectedComponents()

# Analyze the components
print("\n✓ Connected Components complete!")
print("\nComponent statistics:")

component_stats = (
    result
    .groupBy("component")
    .agg(
        F.count("*").alias("size"),
        F.collect_set("Type").alias("node_types")
    )
    .orderBy(F.desc("size"))
)

component_stats.show(10, truncate=False)

# Show total number of components
total_components = component_stats.count()
print(f"\nTotal components found: {total_components:,}")

# Show component size distribution
print("\nComponent size distribution:")
(
    component_stats
    .groupBy("size")
    .count()
    .orderBy("size")
    .withColumnRenamed("count", "num_components")
    .show(20)
)

# Prepare data to write back to Neo4j
print("\n[6/6] Writing component IDs back to Neo4j...")

# Select just id and component for writing back
component_results = result.select("id", "component")

# Write back to Neo4j by updating nodes with component property
(
    component_results.write
    .format("org.neo4j.spark.DataSource")
    .mode("Append")
    .option("query", """
        UNWIND $rows AS row
        MATCH (n {id: row.id})
        SET n.component = row.component
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
verification.show()

print("\n" + "=" * 80)
print("✓ PIPELINE COMPLETE!")
print("=" * 80)
print("\nNext steps:")
print("1. Open Neo4j Browser: http://localhost:7474")
print("2. Run: MATCH (n) WHERE n.component IS NOT NULL RETURN n.component, count(*) ORDER BY count(*) DESC")
print("3. Visualize components: MATCH (n {component: <component_id>})-[r]-(m) RETURN n,r,m LIMIT 100")

spark.stop()
```

Run the connected components pipeline:

```bash
spark-submit \
  --packages org.neo4j:neo4j-connector-apache-spark_2.12:5.3.1_for_spark_3 \
  --driver-memory 4g \
  --executor-memory 4g \
  python/graphframes/tutorials/neo4j/connected_components.py
```

## Step 6: Explore Results in Neo4j

After running the pipeline, open Neo4j Browser (<http://localhost:7474>) and explore the results:

### Find the Largest Component

```cypher
MATCH (n)
WHERE n.component IS NOT NULL
RETURN n.component as componentId, count(*) as size
ORDER BY size DESC
LIMIT 10;
```

### Visualize a Component

```cypher
// Replace <componentId> with an actual component ID from above
MATCH (n {component: <componentId>})-[r]-(m)
RETURN n, r, m
LIMIT 100;
```

### Analyze Component Composition

```cypher
MATCH (n)
WHERE n.component IS NOT NULL
RETURN
    n.component as componentId,
    labels(n)[0] as nodeType,
    count(*) as count
ORDER BY componentId, count DESC;
```

### Find Questions in the Same Component

```cypher
MATCH (q1:Question)-[:Links*1..3]-(q2:Question)
WHERE q1.component = q2.component
  AND q1.component IS NOT NULL
  AND id(q1) < id(q2)
RETURN
    q1.Title as question1,
    q2.Title as question2,
    q1.component as componentId
LIMIT 10;
```

## Understanding the Results

**Connected Components** identifies isolated subgraphs in your network. In the Stack Exchange graph:

- **Large main component**: Most users, questions, answers, votes, and badges are interconnected
- **Small components**: Isolated questions without answers, orphaned users, etc.
- **Component ID**: A unique identifier for each connected subgraph

This is useful for:

- **Data quality**: Finding orphaned or disconnected data
- **Community detection**: Identifying isolated discussion threads
- **Graph structure analysis**: Understanding network connectivity

## Advanced: Custom Graph Algorithms

You can use this same pattern for other GraphFrames algorithms:

### PageRank

```python
# Run PageRank
results = g.pageRank(resetProbability=0.15, maxIter=10)

# Write back to Neo4j
(
    results.vertices
    .select("id", "pagerank")
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Append")
    .option("query", """
        UNWIND $rows AS row
        MATCH (n {id: row.id})
        SET n.pagerank = row.pagerank
    """)
    .save()
)
```

### Triangle Count

```python
# Run Triangle Count
results = g.triangleCount()

# Write back to Neo4j
(
    results
    .select("id", "count")
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Append")
    .option("query", """
        UNWIND $rows AS row
        MATCH (n {id: row.id})
        SET n.triangleCount = row.count
    """)
    .save()
)
```

## Handling Multi-Type Schemas

Notice how we handle GraphFrames' unified schema (all node types in one DataFrame) with Neo4j's label-based type system:

**GraphFrames approach** (from Network Motif Tutorial):

```python
# All nodes in one DataFrame with 'Type' column
nodes_df.select("Type", "id", "Name", "Score", ...).show()
```

**Neo4j approach**:

```cypher
// Nodes have labels
(:Question {id: "...", Title: "..."})
(:User {id: "...", DisplayName: "..."})
(:Badge {id: "...", Name: "..."})
```

**Bridging the gap**:

1. **Writing to Neo4j**: Use `Type` column to set node labels
2. **Reading from Neo4j**: Use `labels(n)[0]` to recreate `Type` column
3. **Unified queries**: Use Cypher to select fields common across types

## Performance Tips

1. **Batch writes**: Write data in batches for better performance
2. **Indexes in Neo4j**: Create indexes on frequently queried properties
3. **Partitioning**: Repartition DataFrames before writing to Neo4j
4. **Memory**: Increase heap size for large graphs
5. **Checkpointing**: Always set checkpoint directory for iterative algorithms

Create indexes in Neo4j:

```cypher
CREATE INDEX node_id_index FOR (n:Node) ON (n.id);
CREATE INDEX question_component FOR (q:Question) ON (q.component);
CREATE INDEX user_component FOR (u:User) ON (u.component);
```

## Cleanup

When you're done, stop and remove the Neo4j container:

```bash
# Stop the container
docker stop neo4j-graphframes

# Remove the container
docker rm neo4j-graphframes

# Optional: Remove data directory
rm -rf /tmp/neo4j-data
```

## Conclusion

In this tutorial, you learned how to:

✓ Set up Neo4j with Docker for graph storage
✓ Load GraphFrames-compatible data into Neo4j
✓ Read graph data from Neo4j into PySpark
✓ Create GraphFrames from Neo4j data
✓ Run distributed graph algorithms (Connected Components)
✓ Write enriched results back to Neo4j
✓ Query and visualize results in Neo4j Browser

This bidirectional integration enables powerful workflows:

- Use **Neo4j** for ACID transactions and real-time queries
- Use **GraphFrames** for batch analytics and ML pipelines
- Combine both for comprehensive graph data platforms

### Next Steps

- Explore other GraphFrames algorithms (PageRank, Label Propagation, Shortest Paths)
- Build streaming pipelines with Spark Structured Streaming
- Integrate with Spark MLlib for graph-based machine learning
- Scale to production with Neo4j Aura and Databricks/EMR

### Additional Resources

- [Neo4j Spark Connector Docs](https://neo4j.com/docs/spark/current/)
- [GraphFrames User Guide](https://graphframes.io/04-user-guide/01-creating-graphframes.html)
- [Neo4j Cypher Manual](https://neo4j.com/docs/cypher-manual/current/)
- [Network Motif Tutorial](02-motif-tutorial.md)
- [Pregel Tutorial](03-pregel-tutorial.md)
