"""Round-trip a Stack Exchange graph between Neo4j and GraphFrames. Code from the Neo4j Integration Tutorial."""  # noqa: E501

#
# Requires a running Neo4j. See the tutorial for the `docker run` command.
#
# Spark 4.0+ (recommended):
#   Interactive: pyspark --packages io.graphframes:graphframes-spark4_2.13:0.12.1,\
#                    org.neo4j:neo4j-connector-apache-spark_2.13:6.0.0_for_spark_4
#   Batch:       spark-submit --packages io.graphframes:graphframes-spark4_2.13:0.12.1,\
#                    org.neo4j:neo4j-connector-apache-spark_2.13:6.0.0_for_spark_4 \
#                    python/graphframes/tutorials/neo4j.py
#
# Spark 3.5.x:
#   Interactive: pyspark --packages io.graphframes:graphframes-spark3_2.13:0.12.1,\
#                    org.neo4j:neo4j-connector-apache-spark_2.13:6.0.0_for_spark_3
#   Batch:       spark-submit --packages io.graphframes:graphframes-spark3_2.13:0.12.1,\
#                    org.neo4j:neo4j-connector-apache-spark_2.13:6.0.0_for_spark_3 \
#                    python/graphframes/tutorials/neo4j.py
#
# The script also sets spark.jars.packages itself, so plain `python neo4j.py` works too.
#

from pathlib import Path
from typing import Dict, List

import click
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from graphframes import GraphFrame

# The Neo4j Spark Connector's DataSource name, used for every read and write below.
NEO4J_FORMAT = "org.neo4j.spark.DataSource"

# Every node gets this label in addition to its Stack Exchange type label. One shared label
# with one uniqueness constraint gives us a single indexed lookup key for the whole graph,
# which is what makes the relationship load and the write-back cheap. Stack Exchange edges
# have heterogeneous endpoints - a Vote is CastFor a Question *or* an Answer - and the
# connector matches endpoints against one fixed label set per write, so without a shared
# label we would need a separate write per (source type, relationship, target type) triple.
SHARED_LABEL = "Node"

DEFAULT_DATA_DIR = str(Path(__file__).parent / "data")

# Package versions used when this script starts its own SparkSession.
GRAPHFRAMES_PACKAGE = "io.graphframes:graphframes-spark4_2.13:0.12.1"
NEO4J_PACKAGE = "org.neo4j:neo4j-connector-apache-spark_2.13:6.0.0_for_spark_4"


def populated_columns(df: DataFrame) -> List[str]:
    """Return the columns of df that hold at least one non-null value.

    Nodes.parquet uses a unified schema: every node type carries every column of every other
    type, almost all of them null. Projecting each type down to its own populated columns
    keeps the Neo4j model readable and cuts the amount of data we push over Bolt.
    """
    non_null_counts = df.select([F.count(F.col(c)).alias(c) for c in df.columns]).first()
    return [c for c in df.columns if non_null_counts[c] > 0]


def create_constraint(spark: SparkSession, neo4j: Dict[str, str]) -> None:
    """Create the :Node(id) uniqueness constraint that every other step depends on.

    Without it, the MERGEs below and the endpoint matching in load_relationships() both
    degrade into full label scans, which turns a two-minute load into an overnight one.

    We ask for it on a write of an *empty* DataFrame for two reasons. The connector can only
    create constraints as a side effect of a write, and its schema-optimization code has no
    mapping for ArrayType as of 6.0.0 - passing 'schema.optimization.node.keys' on a write
    that includes an array column (Question.Tags, here) fails with
    'key not found: ArrayType(StringType,true)'. An empty write creates the constraint,
    creates no nodes, and lets every later write omit the option.
    """
    empty: DataFrame = spark.createDataFrame(
        [], StructType([StructField("id", StringType(), False)])
    )
    (
        empty.write.format(NEO4J_FORMAT)
        .options(**neo4j)
        .mode("Overwrite")
        .option("labels", f":{SHARED_LABEL}")
        .option("node.keys", "id")
        .option("schema.optimization.node.keys", "UNIQUE")
        .save()
    )


def load_nodes(nodes_df: DataFrame, neo4j: Dict[str, str]) -> None:
    """Write each node type into Neo4j as :Node:<Type>, keyed on the GraphFrames UUID 'id'."""
    node_types: List[str] = sorted(
        row["Type"] for row in nodes_df.select("Type").distinct().collect()
    )

    for node_type in node_types:
        type_nodes: DataFrame = nodes_df.filter(F.col("Type") == node_type)

        # Drop the columns this type never populates.
        type_nodes = type_nodes.select(*populated_columns(type_nodes)).cache()
        count: int = type_nodes.count()
        click.echo(
            f"  Loading {count:,} {node_type} nodes ({len(type_nodes.columns)} properties)..."
        )

        (
            type_nodes.write.format(NEO4J_FORMAT)
            .options(**neo4j)
            # 'Overwrite' makes the connector MERGE on node.keys, so re-running this script
            # updates nodes instead of duplicating them. 'Append' would CREATE blindly and
            # silently ignore node.keys.
            .mode("Overwrite")
            .option("labels", f":{SHARED_LABEL}:{node_type}")
            .option("node.keys", "id")
            .save()
        )
        type_nodes.unpersist()
        click.echo(f"    ✓ {node_type}")


def load_relationships(edges_df: DataFrame, neo4j: Dict[str, str]) -> None:
    """Write each relationship type into Neo4j, matching endpoints on :Node(id)."""
    rel_types: List[str] = sorted(
        row["relationship"] for row in edges_df.select("relationship").distinct().collect()
    )

    for rel_type in rel_types:
        # 'relationship' becomes the Neo4j relationship type, so drop it from the payload;
        # any column left over would be written as a relationship property.
        type_edges: DataFrame = edges_df.filter(F.col("relationship") == rel_type).drop(
            "relationship"
        )
        count: int = type_edges.count()
        click.echo(f"  Loading {count:,} {rel_type} relationships...")

        # Write from a single partition. Stack Exchange has dense nodes - one popular question
        # collects thousands of CastFor votes - and concurrent Spark partitions attaching
        # relationships to the same node fight over its relationship-group lock. In separate
        # transactions acquiring those locks in different orders, that is a deadlock: Neo4j
        # kills one transaction with a TransientException and Spark fails the whole job.
        # One writer cannot deadlock against itself.
        #
        # This is the throughput ceiling of this load. To go faster on a real graph, keep the
        # parallelism but make sure no two partitions touch the same dense node - repartition
        # by whichever endpoint is the dense one, per relationship type - and raise
        # 'transaction.retries' so the occasional loser is retried instead of fatal.
        type_edges = type_edges.coalesce(1)

        (
            type_edges.write.format(NEO4J_FORMAT)
            .options(**neo4j)
            .mode("Overwrite")
            .option("relationship", rel_type)
            # 'keys' means: match the endpoints by the key columns we name below. Both
            # endpoints match on the shared :Node label, which is what lets one write cover
            # every source/target type combination this relationship connects.
            .option("relationship.save.strategy", "keys")
            .option("relationship.source.labels", f":{SHARED_LABEL}")
            .option("relationship.source.save.mode", "Match")
            .option("relationship.source.node.keys", "src:id")
            .option("relationship.target.labels", f":{SHARED_LABEL}")
            .option("relationship.target.save.mode", "Match")
            .option("relationship.target.node.keys", "dst:id")
            .save()
        )
        click.echo(f"    ✓ {rel_type}")


def read_graph(spark: SparkSession, neo4j: Dict[str, str], partitions: int) -> GraphFrame:
    """Read the graph back out of Neo4j into a GraphFrame.

    Reading with Cypher via the 'query' option gives us exactly the GraphFrames column names
    we need - id, src, dst, relationship - with no post-processing.
    """
    vertices: DataFrame = (
        spark.read.format(NEO4J_FORMAT)
        .options(**neo4j)
        .option("query", f"MATCH (n:{SHARED_LABEL}) RETURN n.id AS id, n.Type AS Type")
        # A 'query' read is single-partition unless we tell the connector how many rows to
        # expect, so give it a count query and a partition count.
        .option("query.count", f"MATCH (n:{SHARED_LABEL}) RETURN count(n) AS count")
        .option("partitions", str(partitions))
        .load()
    )

    edges: DataFrame = (
        spark.read.format(NEO4J_FORMAT)
        .options(**neo4j)
        .option(
            "query",
            f"MATCH (s:{SHARED_LABEL})-[r]->(t:{SHARED_LABEL}) "
            "RETURN s.id AS src, t.id AS dst, type(r) AS relationship",
        )
        .option(
            "query.count",
            f"MATCH (:{SHARED_LABEL})-[r]->(:{SHARED_LABEL}) RETURN count(r) AS count",
        )
        .option("partitions", str(partitions))
        .load()
    )

    return GraphFrame(vertices, edges)


def write_components(components_df: DataFrame, neo4j: Dict[str, str]) -> None:
    """Write component IDs back onto the existing Neo4j nodes.

    Matching on the shared label alone (:Node, not :Node:Question) means one write updates
    every node type. Because 'Overwrite' MERGEs on :Node(id) and every id already exists,
    this sets the new property in place - it does not create nodes, drop the per-type labels,
    or touch the properties we loaded earlier.
    """
    (
        components_df.write.format(NEO4J_FORMAT)
        .options(**neo4j)
        .mode("Overwrite")
        .option("labels", f":{SHARED_LABEL}")
        .option("node.keys", "id")
        .save()
    )


def cypher(spark: SparkSession, neo4j: Dict[str, str], query: str) -> DataFrame:
    """Run a read-only Cypher query and return the result as a DataFrame.

    The connector rewrites the query to partition it, so it rejects a trailing SKIP/LIMIT.
    Take the top N with DataFrame.limit() instead of in Cypher.
    """
    return spark.read.format(NEO4J_FORMAT).options(**neo4j).option("query", query).load()


@click.command()
@click.option(
    "--data-dir",
    default=DEFAULT_DATA_DIR,
    help="Directory containing Stack Exchange Parquet data (default: package data directory)",
)
@click.option(
    "--site",
    default="stats.meta.stackexchange.com",
    help="Stack Exchange site subdirectory to load",
)
@click.option("--neo4j-url", default="neo4j://localhost:7687", help="Neo4j Bolt URL")
@click.option("--neo4j-user", default="neo4j", help="Neo4j username")
@click.option("--neo4j-password", default="graphframes123", help="Neo4j password")
@click.option("--neo4j-database", default="neo4j", help="Neo4j database name")
@click.option("--partitions", default=8, help="Partitions to use when reading back from Neo4j")
def main(
    data_dir: str,
    site: str,
    neo4j_url: str,
    neo4j_user: str,
    neo4j_password: str,
    neo4j_database: str,
    partitions: int,
) -> None:
    spark: SparkSession = (
        SparkSession.builder.appName("Neo4j + GraphFrames: Stack Exchange")
        .config("spark.jars.packages", f"{GRAPHFRAMES_PACKAGE},{NEO4J_PACKAGE}")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    # Connected Components checkpoints, so Spark needs somewhere to put the checkpoints.
    spark.sparkContext.setCheckpointDir("/tmp/graphframes-checkpoints/neo4j")

    # Passed to every Neo4j read and write. As DataFrame options these drop the 'neo4j.'
    # prefix they would carry as Spark configs.
    neo4j: Dict[str, str] = {
        "url": neo4j_url,
        "authentication.basic.username": neo4j_user,
        "authentication.basic.password": neo4j_password,
        "database": neo4j_database,
    }

    base_path: str = f"{data_dir}/{site}"

    #
    # Step 1: load the GraphFrames Parquet files we built in stackexchange.py
    #

    click.echo("\n=== Reading Parquet ===")
    nodes_df: DataFrame = spark.read.parquet(f"{base_path}/Nodes.parquet").cache()
    edges_df: DataFrame = spark.read.parquet(f"{base_path}/Edges.parquet").cache()
    click.echo(f"Nodes: {nodes_df.count():,}")
    click.echo(f"Edges: {edges_df.count():,}")

    nodes_df.groupBy("Type").count().orderBy(F.desc("count")).withColumn(
        "count", F.format_number(F.col("count"), 0)
    ).show()
    edges_df.groupBy("relationship").count().orderBy(F.desc("count")).withColumn(
        "count", F.format_number(F.col("count"), 0)
    ).show()

    #
    # Step 2: load the nodes into Neo4j, one type at a time
    #

    click.echo("=== Loading nodes into Neo4j ===")
    create_constraint(spark, neo4j)
    click.echo(f"  ✓ uniqueness constraint on :{SHARED_LABEL}(id)")
    load_nodes(nodes_df, neo4j)

    #
    # Step 3: load the relationships. Nodes must exist first - the endpoints are matched,
    # not created, so any relationship whose endpoints are missing is silently dropped.
    #

    click.echo("\n=== Loading relationships into Neo4j ===")
    load_relationships(edges_df, neo4j)

    nodes_df.unpersist()
    edges_df.unpersist()

    click.echo("\n=== Verifying the load ===")
    cypher(
        spark,
        neo4j,
        f"MATCH (n:{SHARED_LABEL}) RETURN n.Type AS Type, count(*) AS count ORDER BY count DESC",
    ).show()
    cypher(
        spark,
        neo4j,
        f"MATCH (:{SHARED_LABEL})-[r]->(:{SHARED_LABEL}) "
        "RETURN type(r) AS relationship, count(*) AS count ORDER BY count DESC",
    ).show()

    #
    # Step 4: read the graph back out of Neo4j and run Connected Components on Spark
    #

    click.echo("=== Reading the graph back into GraphFrames ===")
    graph: GraphFrame = read_graph(spark, neo4j, partitions)
    graph.vertices.cache()
    graph.edges.cache()
    click.echo(f"Vertices read from Neo4j: {graph.vertices.count():,}")
    click.echo(f"Edges read from Neo4j:    {graph.edges.count():,}")

    click.echo("\n=== Running Connected Components ===")
    components: DataFrame = graph.connectedComponents().cache()

    component_count: int = components.select("component").distinct().count()
    click.echo(f"Connected components found: {component_count:,}")

    click.echo("\nLargest components:")
    (
        components.groupBy("component")
        .count()
        .orderBy(F.desc("count"))
        .limit(10)
        .withColumn("count", F.format_number(F.col("count"), 0))
        .show(truncate=False)
    )

    click.echo("What types of node make up the largest component?")
    largest = components.groupBy("component").count().orderBy(F.desc("count")).first()["component"]
    (
        components.filter(F.col("component") == largest)
        .groupBy("Type")
        .count()
        .orderBy(F.desc("count"))
        .withColumn("count", F.format_number(F.col("count"), 0))
        .show()
    )

    #
    # Step 5: write the component IDs back onto the Neo4j nodes
    #

    click.echo("=== Writing component IDs back to Neo4j ===")
    # The connector writes every column it is given; send only the key and the new property.
    write_components(components.select("id", F.col("component").alias("componentId")), neo4j)
    click.echo("  ✓ componentId written")

    click.echo("\n=== Verifying the write-back ===")
    cypher(
        spark,
        neo4j,
        f"MATCH (n:{SHARED_LABEL}) WHERE n.componentId IS NOT NULL "
        "RETURN count(n) AS nodesWithComponentId",
    ).show()
    cypher(
        spark,
        neo4j,
        f"MATCH (n:{SHARED_LABEL}) RETURN n.componentId AS componentId, count(*) AS size "
        "ORDER BY size DESC",
    ).limit(5).show()

    # The component IDs are now queryable in Neo4j alongside everything else. This finds the
    # users in the largest component with the most badges - a query that mixes the property
    # graph with a result Spark computed.
    click.echo("Most decorated users in the largest component:")
    cypher(
        spark,
        neo4j,
        f"MATCH (u:User)-[:Earns]->(b:Badge) WHERE u.componentId = {largest} "
        "RETURN u.DisplayName AS user, count(b) AS badges ORDER BY badges DESC",
    ).limit(10).show(truncate=False)

    components.unpersist()
    spark.stop()
    click.echo("Spark stopped.")


if __name__ == "__main__":
    main()
