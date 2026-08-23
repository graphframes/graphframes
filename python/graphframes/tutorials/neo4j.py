#!/usr/bin/env python

"""Read a graph out of Neo4j, run Connected Components on Spark, write the results back.

Code from the Neo4j Integration Tutorial. Run it once the graph is loaded:

    graphframes neo4j setup
    graphframes neo4j load
    spark-submit --packages io.graphframes:graphframes-spark4_2.13:0.12.1,\
org.neo4j:neo4j-connector-apache-spark_2.13:6.0.0_for_spark_4 \
        python/graphframes/tutorials/neo4j.py

On Spark 3.5 use graphframes-spark3_2.13:0.12.1 and 6.0.0_for_spark_3 instead.
"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from graphframes import GraphFrame

# The Neo4j Spark Connector's DataSource name, used for every read and write below.
NEO4J_FORMAT = "org.neo4j.spark.DataSource"

# Connection settings. As DataFrame options these drop the 'neo4j.' prefix they would carry
# as Spark configs, and spark-submit does not warn about each one.
NEO4J = {
    "url": "neo4j://localhost:7687",
    "authentication.basic.username": "neo4j",
    "authentication.basic.password": "graphframes123",
    "database": "neo4j",
}

# `graphframes neo4j load` gives every node this label alongside its Stack Exchange type,
# and a uniqueness constraint on :Node(id). That one index serves every read and write here.
LABEL = "Node"

# A 'query' read is single-partition unless we tell the connector how many rows to expect.
PARTITIONS = 8

spark = SparkSession.builder.appName("Neo4j + GraphFrames").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
# Connected Components checkpoints as it iterates, so Spark needs somewhere to put them.
spark.sparkContext.setCheckpointDir("/tmp/graphframes-checkpoints/neo4j")


def cypher(query: str, count_query: str = "") -> DataFrame:
    """Run a read-only Cypher query and return the result as a DataFrame.

    Pass count_query to spread the read across PARTITIONS workers. The connector rewrites
    the query to partition it, so it rejects a trailing SKIP/LIMIT - use DataFrame.limit().
    """
    reader = spark.read.format(NEO4J_FORMAT).options(**NEO4J).option("query", query)
    if count_query:
        reader = reader.option("query.count", count_query).option("partitions", str(PARTITIONS))
    return reader.load()


#
# 1. Read the graph. The Cypher aliases are exactly the column names GraphFrames expects,
#    so there is nothing to rename afterwards.
#

vertices = cypher(
    f"MATCH (n:{LABEL}) RETURN n.id AS id, n.Type AS Type",
    f"MATCH (n:{LABEL}) RETURN count(n) AS count",
)
edges = cypher(
    f"MATCH (s:{LABEL})-[r]->(t:{LABEL}) "
    "RETURN s.id AS src, t.id AS dst, type(r) AS relationship",
    f"MATCH (:{LABEL})-[r]->(:{LABEL}) RETURN count(r) AS count",
)

graph = GraphFrame(vertices, edges)
print(f"Vertices: {graph.vertices.count():,}   Edges: {graph.edges.count():,}")

#
# 2. Run Connected Components - the expensive, iterative job we came to Spark for.
#

components = graph.connectedComponents().select("id", "Type", "component").cache()
print(f"Components found: {components.select('component').distinct().count():,}")

largest = components.groupBy("component").count().orderBy(F.desc("count")).first()["component"]
print("The largest component, by node type:")
components.filter(F.col("component") == largest).groupBy("Type").count().orderBy(
    F.desc("count")
).show()

#
# 3. Write the component IDs back. 'Overwrite' makes the connector MERGE on node.keys, so this
#    upserts a 'component' property onto nodes that already exist: it creates nothing, drops no
#    labels and disturbs no other property. Matching the shared :Node label alone updates every
#    node type in one write, and the :Node(id) constraint keeps the MERGE index-backed instead
#    of a scan of all 129,751 nodes. Send only the key and the new property - the connector
#    writes every column it is given.
#

(
    components.select("id", "component")
    .write.format(NEO4J_FORMAT)
    .options(**NEO4J)
    .mode("Overwrite")
    .option("labels", f":{LABEL}")
    .option("node.keys", "id")
    .save()
)

#
# 4. The component IDs are queryable in Neo4j alongside everything else now. This asks a
#    question that mixes the property graph with the result Spark just computed.
#

print("Most decorated users in the largest component:")
cypher(
    f"MATCH (u:User)-[:Earns]->(b:Badge) WHERE u.component = {largest} "
    "RETURN u.DisplayName AS user, count(b) AS badges ORDER BY badges DESC"
).limit(10).show(truncate=False)

spark.stop()
