#!/usr/bin/env python

"""Read a graph out of Neo4j, run Connected Components on Spark, write the results back.

Code from the Neo4j Integration Tutorial. Run it once the graph is loaded:

    graphframes neo4j setup
    graphframes neo4j load
    spark-submit --packages io.graphframes:graphframes-spark4_2.13:0.12.1,\
org.neo4j:neo4j-connector-apache-spark_2.13:6.0.0_for_spark_4 \
        python/graphframes/tutorials/neo4j.py

Connection settings default to what `graphframes neo4j setup` creates. If you set Neo4j up
with different ones - `graphframes neo4j setup --password hunter2`, say - pass them through
the environment:

    export NEO4J_PASSWORD=hunter2
    spark-submit --packages ... python/graphframes/tutorials/neo4j.py

or as arguments, which take precedence over the environment:

    spark-submit --packages ... python/graphframes/tutorials/neo4j.py --password hunter2

On Spark 3.5 use graphframes-spark3_2.13:0.12.1 and 6.0.0_for_spark_3 instead.
"""

import argparse
import os
import time

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from graphframes.pg import EdgePropertyGroup, PropertyGraphFrame, VertexPropertyGroup

# The Neo4j Spark Connector's DataSource name, used for every read and write below.
NEO4J_FORMAT = "org.neo4j.spark.DataSource"

# `graphframes neo4j load` gives every node this label alongside its Stack Exchange type,
# and a uniqueness constraint on :Node(id). That one index serves every read and write here.
LABEL = "Node"

# A 'query' read is single-partition unless we tell the connector how many rows to expect.
PARTITIONS = 8

# The eight relationship types `graphframes neo4j load` creates - see the tables in the
# Data Setup tutorial and Step 3 below. Each becomes its own EdgePropertyGroup, which is
# what makes this a PropertyGraphFrame instead of one flat, heterogeneous edge DataFrame.
RELATIONSHIP_TYPES = ["Earns", "CastFor", "Tags", "Answers", "Posts", "Asks", "Links", "Duplicates"]

# Defaults match `graphframes neo4j setup`. The password is a throwaway for a local tutorial
# container, so it is a default rather than a hard-coded constant: anything real belongs in
# the environment, and NEO4J_PASSWORD keeps it out of both this file and your shell history.
DEFAULTS = {
    "url": os.environ.get("NEO4J_URL", "neo4j://localhost:7687"),
    "username": os.environ.get("NEO4J_USERNAME", "neo4j"),
    "password": os.environ.get("NEO4J_PASSWORD", "graphframes123"),
    "database": os.environ.get("NEO4J_DATABASE", "neo4j"),
}

parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
parser.add_argument("--url", default=DEFAULTS["url"], help="Neo4j Bolt URL")
parser.add_argument("--username", default=DEFAULTS["username"], help="Neo4j username")
parser.add_argument(
    "--password",
    default=DEFAULTS["password"],
    help="Neo4j password (prefer the NEO4J_PASSWORD environment variable)",
)
parser.add_argument("--database", default=DEFAULTS["database"], help="Neo4j database name")
args = parser.parse_args()

# Connection settings. As DataFrame options these drop the 'neo4j.' prefix they would carry
# as Spark configs, and spark-submit does not warn about each one.
NEO4J = {
    "url": args.url,
    "authentication.basic.username": args.username,
    "authentication.basic.password": args.password,
    "database": args.database,
}

_START = time.monotonic()


def step(message: str) -> None:
    """Print an elapsed-time progress line.

    Every stage below announces itself before it starts, so if the script sits for a while
    the last line printed tells you which one it is sitting in. flush=True matters: when
    spark-submit's stdout is a pipe rather than a terminal, buffered prints arrive in a
    lump at exit, which makes a slow stage look like a hang.
    """
    print(f"[{time.monotonic() - _START:7.1f}s] {message}", flush=True)


def lingering_non_daemon_threads(jvm) -> list[str]:
    """Names of live non-daemon JVM threads, which are what keep a JVM from exiting.

    The JVM exits once its last non-daemon thread finishes. Anything still listed here
    after Spark has stopped is what stands between this script and a clean exit.
    """
    return sorted(
        {
            thread.getName()
            for thread in jvm.java.lang.Thread.getAllStackTraces().keySet()
            if thread.isAlive() and not thread.isDaemon() and thread.getName() != "DestroyJavaVM"
        }
    )


step(f"Connecting to {args.url} as {args.username!r}, database {args.database!r}.")

spark = SparkSession.builder.appName("Neo4j + GraphFrames").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
# Connected Components checkpoints as it iterates, so Spark needs somewhere to put them.
spark.sparkContext.setCheckpointDir("/tmp/graphframes-checkpoints/neo4j")

# Held for the shutdown check at the end, which runs after the session is stopped.
jvm = spark._jvm


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
#    so there is nothing to rename afterwards. Cache both: `vertices` is filtered seven
#    ways below (once per node type) and `edges` eight ways (once per relationship type) -
#    without caching, each filter would otherwise re-run the Cypher read against Neo4j.
#

vertices = cypher(
    f"MATCH (n:{LABEL}) RETURN n.id AS id, n.Type AS Type",
    f"MATCH (n:{LABEL}) RETURN count(n) AS count",
).cache()
edges = cypher(
    f"MATCH (s:{LABEL})-[r]->(t:{LABEL}) "
    "RETURN s.id AS src, t.id AS dst, type(r) AS relationship",
    f"MATCH (:{LABEL})-[r]->(:{LABEL}) RETURN count(r) AS count",
).cache()


def _nodes_of_type(node_type: str) -> DataFrame:
    return vertices.filter(F.col("Type") == node_type).select("id")


def _edges_of_type(relationship: str) -> DataFrame:
    return edges.filter(F.col("relationship") == relationship).select("src", "dst")


def _edge_group(relationship: str, src_group: VertexPropertyGroup, dst_group: VertexPropertyGroup):
    return EdgePropertyGroup(
        relationship,
        _edges_of_type(relationship),
        src_group,
        dst_group,
        is_directed=True,
        src_column_name="src",
        dst_column_name="dst",
    )


#
# 2. Model the graph as a PropertyGraphFrame instead of handing raw DataFrames straight to
#    GraphFrame: one VertexPropertyGroup per real Stack Exchange node type - ids are already
#    globally-unique UUIDs, so apply_mask_on_id=False skips hashing them, and each group is
#    named after its Type so it doubles as one after the fact (see step 4 below). CastFor,
#    Tags, Links and Duplicates all connect to a Post - Question or Answer, Stack Exchange's
#    word for either - so `posts` exists purely to give those four relationship types one
#    dst/src group to name instead of the two each could mean; it is never itself part of a
#    vertex projection below, only referenced by the edge groups that need it.
#

users = VertexPropertyGroup("User", _nodes_of_type("User"), "id", apply_mask_on_id=False)
badges = VertexPropertyGroup("Badge", _nodes_of_type("Badge"), "id", apply_mask_on_id=False)
votes = VertexPropertyGroup("Vote", _nodes_of_type("Vote"), "id", apply_mask_on_id=False)
questions = VertexPropertyGroup(
    "Question", _nodes_of_type("Question"), "id", apply_mask_on_id=False
)
answers = VertexPropertyGroup("Answer", _nodes_of_type("Answer"), "id", apply_mask_on_id=False)
post_links = VertexPropertyGroup(
    "PostLinks", _nodes_of_type("PostLinks"), "id", apply_mask_on_id=False
)
tags = VertexPropertyGroup("Tag", _nodes_of_type("Tag"), "id", apply_mask_on_id=False)
post_data = vertices.filter(F.col("Type").isin("Question", "Answer")).select("id")
posts = VertexPropertyGroup("Post", post_data, "id", apply_mask_on_id=False)

NODE_TYPES = [
    users.name,
    badges.name,
    votes.name,
    questions.name,
    answers.name,
    post_links.name,
    tags.name,
]

property_graph = PropertyGraphFrame(
    [users, badges, votes, questions, answers, post_links, tags, posts],
    [
        _edge_group("Earns", users, badges),
        _edge_group("CastFor", votes, posts),
        _edge_group("Tags", tags, posts),
        _edge_group("Answers", answers, questions),
        _edge_group("Posts", users, answers),
        _edge_group("Asks", users, questions),
        _edge_group("Links", posts, posts),
        _edge_group("Duplicates", posts, posts),
    ],
)

#
# 3. Project the PropertyGraphFrame down to a GraphFrame. to_graphframe() always takes an
#    explicit list of vertex/edge property groups rather than assuming "everything" - here
#    that projection is every real node type and every relationship, because Connected
#    Components is meant to answer "how does the whole graph connect". A narrower question -
#    say, how users, questions and answers connect through content alone - would instead
#    pass edge_property_groups=["Asks", "Posts", "Answers"] and leave Votes and Badges out.
#

graph = property_graph.to_graphframe(
    vertex_property_groups=NODE_TYPES, edge_property_groups=RELATIONSHIP_TYPES
)
step(
    "Counting vertices and edges - the first read, so this is where a bad URL or password shows up."
)
# validate() catches the two things that would otherwise fail silently or blow up mid-run:
# duplicate vertex ids, and edges pointing at a vertex that does not exist.
graph.validate()
print(f"Vertices: {graph.vertices.count():,}   Edges: {graph.edges.count():,}")

#
# 4. Run Connected Components - the expensive, iterative job we came to Spark for.
#

step("Running Connected Components. This is the long one - minutes, not seconds.")
# Each vertex group above is named after its Type, so to_graphframe()'s "property_group"
# column - which connectedComponents() carries through untouched alongside the new
# "component" column - already *is* Type. No join back to `vertices` is needed.
components = (
    graph.connectedComponents()
    .withColumnRenamed("property_group", "Type")
    .select("id", "Type", "component")
    .cache()
)
print(f"Components found: {components.select('component').distinct().count():,}")

step("Finding the largest component.")
largest = components.groupBy("component").count().orderBy(F.desc("count")).first()["component"]
print("The largest component, by node type:")
components.filter(F.col("component") == largest).groupBy("Type").count().orderBy(
    F.desc("count")
).show()

#
# 5. Write the component IDs back. 'Overwrite' makes the connector MERGE on node.keys, so this
#    upserts a 'component' property onto nodes that already exist: it creates nothing, drops no
#    labels and disturbs no other property. Matching the shared :Node label alone updates every
#    node type in one write, and the :Node(id) constraint keeps the MERGE index-backed instead
#    of a scan of all 129,751 nodes. Send only the key and the new property - the connector
#    writes every column it is given.
#

step("Writing component IDs back to Neo4j.")
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
# 6. The component IDs are queryable in Neo4j alongside everything else now. This asks a
#    question that mixes the property graph with the result Spark just computed.
#

step("Querying the result back out of Neo4j.")
print("Most decorated users in the largest component:")
cypher(
    f"MATCH (u:User)-[:Earns]->(b:Badge) WHERE u.component = {largest} "
    "RETURN u.DisplayName AS user, count(b) AS badges ORDER BY badges DESC"
).limit(10).show(truncate=False)

#
# 7. Shut down. spark.stop() ends the Spark session but not the Neo4j Bolt driver, whose Netty
#    event loops run on *non-daemon* threads. A JVM will not exit while a non-daemon thread is
#    alive, so on its own this script prints its last result and then sits there forever, with
#    the driver's main thread parked in DestroyJavaVM waiting on threads that never finish.
#    Report whatever is still holding the JVM open, then bring it down deliberately.
#

step("Stopping Spark.")
spark.stop()
step("Spark stopped.")

lingering = lingering_non_daemon_threads(jvm)
if lingering:
    step(f"{len(lingering)} non-daemon JVM thread(s) still alive: {', '.join(lingering)}")
    step("These keep the JVM from exiting (Neo4j's Bolt driver). Exiting explicitly.")
    jvm.System.exit(0)

step("Done.")
