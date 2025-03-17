#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import pytest
from pyspark.sql import functions as sqlfunctions
from pyspark.sql.utils import is_remote

from graphframes.classic.graphframe import _from_java_gf
from graphframes.examples import BeliefPropagation, Graphs
from graphframes.graphframe import GraphFrame
from graphframes.lib import AggregateMessages as AM


def test_construction(spark, local_g):
    vertexIDs = [row[0] for row in local_g.vertices.select("id").collect()]
    assert sorted(vertexIDs) == [1, 2, 3]

    edgeActions = [row[0] for row in local_g.edges.select("action").collect()]
    assert sorted(edgeActions) == ["follow", "hate", "love"]
    tripletsFirst = list(
        map(
            lambda x: (x[0][1], x[1][1], x[2][2]),
            local_g.triplets.sort("src.id").select("src", "dst", "edge").take(1),
        )
    )
    assert tripletsFirst == [("A", "B", "love")], tripletsFirst

    # Try with invalid vertices and edges DataFrames
    v_invalid = spark.createDataFrame(
        [(1, "A"), (2, "B"), (3, "C")], ["invalid_colname_1", "invalid_colname_2"]
    )
    e_invalid = spark.createDataFrame(
        [(1, 2), (2, 3), (3, 1)], ["invalid_colname_3", "invalid_colname_4"]
    )
    with pytest.raises(ValueError):
        GraphFrame(v_invalid, e_invalid)


def test_cache(local_g):
    local_g.cache()
    local_g.unpersist()


def test_degrees(local_g):
    outDeg = local_g.outDegrees
    assert set(outDeg.columns) == {"id", "outDegree"}
    inDeg = local_g.inDegrees
    assert set(inDeg.columns) == {"id", "inDegree"}
    deg = local_g.degrees
    assert set(deg.columns) == {"id", "degree"}


def test_motif_finding(local_g):
    motifs = local_g.find("(a)-[e]->(b)")
    assert motifs.count() == 3
    assert set(motifs.columns) == {"a", "e", "b"}


def test_filterVertices(local_g):
    conditions = ["id < 3", local_g.vertices.id < 3]
    expected_v = [(1, "A"), (2, "B")]
    expected_e = [(1, 2, "love"), (2, 1, "hate")]
    for cond in conditions:
        g2 = local_g.filterVertices(cond)
        v2 = g2.vertices.select("id", "name").collect()
        e2 = g2.edges.select("src", "dst", "action").collect()
        assert len(v2) == len(expected_v)
        assert len(e2) == len(expected_e)
        assert set(v2) == set(expected_v)
        assert set(e2) == set(expected_e)


def test_filterEdges(local_g):
    conditions = ["dst > 2", local_g.edges.dst > 2]
    expected_v = [(1, "A"), (2, "B"), (3, "C")]
    expected_e = [(2, 3, "follow")]
    for cond in conditions:
        g2 = local_g.filterEdges(cond)
        v2 = g2.vertices.select("id", "name").collect()
        e2 = g2.edges.select("src", "dst", "action").collect()
        assert len(v2) == len(expected_v)
        assert len(e2) == len(expected_e)
        assert set(v2) == set(expected_v)
        assert set(e2) == set(expected_e)


def test_dropIsolatedVertices(local_g):
    g2 = local_g.filterEdges("dst > 2").dropIsolatedVertices()
    v2 = g2.vertices.select("id", "name").collect()
    e2 = g2.edges.select("src", "dst", "action").collect()
    expected_v = [(2, "B"), (3, "C")]
    expected_e = [(2, 3, "follow")]
    assert len(v2) == len(expected_v)
    assert len(e2) == len(expected_e)
    assert set(v2) == set(expected_v)
    assert set(e2) == set(expected_e)


def test_bfs(local_g):
    paths = local_g.bfs("name='A'", "name='C'")
    assert paths is not None
    assert paths.count() == 1
    # Expecting that the first intermediary vertex in the BFS is "B"
    head = paths.select("v1.name").head()
    assert head is not None
    assert head[0] == "B"

    paths2 = local_g.bfs("name='A'", "name='C'", edgeFilter="action!='follow'")
    assert paths2.count() == 0

    paths3 = local_g.bfs("name='A'", "name='C'", maxPathLength=1)
    assert paths3.count() == 0


def test_power_iteration_clustering(spark):
    vertices = [
        (1, 0, 0.5),
        (2, 0, 0.5),
        (2, 1, 0.7),
        (3, 0, 0.5),
        (3, 1, 0.7),
        (3, 2, 0.9),
        (4, 0, 0.5),
        (4, 1, 0.7),
        (4, 2, 0.9),
        (4, 3, 1.1),
        (5, 0, 0.5),
        (5, 1, 0.7),
        (5, 2, 0.9),
        (5, 3, 1.1),
        (5, 4, 1.3),
    ]
    edges = [(0,), (1,), (2,), (3,), (4,), (5,)]
    g = GraphFrame(
        v=spark.createDataFrame(edges).toDF("id"),
        e=spark.createDataFrame(vertices).toDF("src", "dst", "weight"),
    )

    clusters = [
        r["cluster"]
        for r in g.powerIterationClustering(k=2, maxIter=40, weightCol="weight")
        .sort("id")
        .collect()
    ]

    assert clusters == [0, 0, 0, 0, 1, 0]


def test_page_rank(spark):
    edges = spark.createDataFrame(
        [
            [0, 1],
            [1, 2],
            [2, 4],
            [2, 0],
            [3, 4],  # 3 has no in-links
            [4, 0],
            [4, 2],
        ],
        ["src", "dst"],
    )
    edges.cache()
    vertices = spark.createDataFrame([[0], [1], [2], [3], [4]], ["id"])
    numVertices = vertices.count()

    vertices = GraphFrame(vertices, edges).outDegrees
    vertices.toPandas().head()
    vertices.cache()

    # Construct a new GraphFrame with the updated vertices DataFrame.
    graph = GraphFrame(vertices, edges)
    alpha = 0.15
    pregel = graph.pregel
    ranks = (
        graph.pregel.setMaxIter(5)
        .withVertexColumn(
            "rank",
            sqlfunctions.lit(1.0 / numVertices),
            sqlfunctions.coalesce(pregel.msg(), sqlfunctions.lit(0.0))
            * sqlfunctions.lit(1.0 - alpha)
            + sqlfunctions.lit(alpha / numVertices),
        )
        .sendMsgToDst(pregel.src("rank") / pregel.src("outDegree"))
        .aggMsgs(sqlfunctions.sum(pregel.msg()))
        .run()
    )
    resultRows = ranks.sort("id").collect()
    result = map(lambda x: x.rank, resultRows)
    expected = [0.245, 0.224, 0.303, 0.03, 0.197]

    # Compare each result with its expected value using a tolerance of 1e-3.
    for a, b in zip(result, expected):
        assert a == pytest.approx(b, abs=1e-3)


def _hasCols(graph, vcols=[], ecols=[]):
    for c in vcols:
        assert c in graph.vertices.columns, f"Vertex DataFrame missing column: {c}"
    for c in ecols:
        assert c in graph.edges.columns, f"Edge DataFrame missing column: {c}"


def _df_hasCols(df, vcols=[]):
    for c in vcols:
        assert c in df.columns, f"DataFrame missing column: {c}"


@pytest.mark.skipif(is_remote(), reason="DISABLE FOR CONNECT")
def test_aggregate_messages(examples, spark):
    g = _from_java_gf(getattr(examples, "friends")(), spark)
    # For each user, sum the ages of the adjacent users,
    # plus 1 for the src's sum if the edge is "friend".
    sendToSrc = AM.dst["age"] + sqlfunctions.when(
        AM.edge["relationship"] == "friend", sqlfunctions.lit(1)
    ).otherwise(0)
    sendToDst = AM.src["age"]
    agg = g.aggregateMessages(
        sqlfunctions.sum(AM.msg).alias("summedAges"),
        sendToSrc=sendToSrc,
        sendToDst=sendToDst,
    )
    # Run the aggregation again using SQL expressions as Strings.
    agg2 = g.aggregateMessages(
        "sum(MSG) AS `summedAges`",
        sendToSrc="(dst['age'] + CASE WHEN (edge['relationship'] = 'friend') THEN 1 ELSE 0 END)",  # noqa: E501
        sendToDst="src['age']",
    )
    # Build mappings from id to the aggregated message.
    aggMap = {row.id: row.summedAges for row in agg.select("id", "summedAges").collect()}
    agg2Map = {row.id: row.summedAges for row in agg2.select("id", "summedAges").collect()}
    # Compute the expected aggregation via brute force.
    user2age = {row.id: row.age for row in g.vertices.select("id", "age").collect()}
    trueAgg = {}
    for src, dst, rel in g.edges.select("src", "dst", "relationship").collect():
        trueAgg[src] = trueAgg.get(src, 0) + user2age[dst] + (1 if rel == "friend" else 0)
        trueAgg[dst] = trueAgg.get(dst, 0) + user2age[src]
    # Verify both aggregations match the expected results.
    assert aggMap == trueAgg, f"aggMap {aggMap} does not equal expected {trueAgg}"
    assert agg2Map == trueAgg, f"agg2Map {agg2Map} does not equal expected {trueAgg}"
    # Check that passing a wrong type for messages raises a TypeError.
    with pytest.raises(TypeError):
        g.aggregateMessages("sum(MSG) AS `summedAges`", sendToSrc=object(), sendToDst="src['age']")
    with pytest.raises(TypeError):
        g.aggregateMessages("sum(MSG) AS `summedAges`", sendToSrc=dst["age"], sendToDst=object())


def test_connected_components(spark):
    v = spark.createDataFrame([(0, "a", "b")], ["id", "vattr", "gender"])
    e = spark.createDataFrame([(0, 0, 1)], ["src", "dst", "test"]).filter("src > 10")
    v = spark.createDataFrame([(0, "a", "b")], ["id", "vattr", "gender"])
    e = spark.createDataFrame([(0, 0, 1)], ["src", "dst", "test"]).filter("src > 10")
    g = GraphFrame(v, e)
    comps = g.connectedComponents()
    _df_hasCols(comps, vcols=["id", "component", "vattr", "gender"])
    assert comps.count() == 1


def test_connected_components2(spark):
    v = spark.createDataFrame([(0, "a0", "b0"), (1, "a1", "b1")], ["id", "A", "B"])
    e = spark.createDataFrame([(0, 1, "a01", "b01")], ["src", "dst", "A", "B"])
    g = GraphFrame(v, e)
    comps = g.connectedComponents()
    _df_hasCols(comps, vcols=["id", "component", "A", "B"])
    assert comps.count() == 2


@pytest.mark.skipif(is_remote(), reason="DISABLE FOR CONNECT")
def test_connected_components_friends(examples, spark):
    g = _from_java_gf(getattr(examples, "friends")(), spark)
    comps_tests = [
        g.connectedComponents(),
        g.connectedComponents(broadcastThreshold=1),
        g.connectedComponents(checkpointInterval=0),
        g.connectedComponents(checkpointInterval=10),
        g.connectedComponents(algorithm="graphx"),
    ]
    for c in comps_tests:
        assert c.groupBy("component").count().count() == 2


@pytest.mark.skipif(is_remote(), reason="DISABLE FOR CONNECT")
def test_label_progagation(examples, spark):
    n = 5
    g = _from_java_gf(getattr(examples, "twoBlobs")(n), spark)
    labels = g.labelPropagation(maxIter=4 * n)
    labels1 = labels.filter("id < 5").select("label").collect()
    all1 = {row.label for row in labels1}
    assert len(all1) == 1
    labels2 = labels.filter("id >= 5").select("label").collect()
    all2 = {row.label for row in labels2}
    assert len(all2) == 1
    assert all1 != all2


@pytest.mark.skipif(is_remote(), reason="DISABLE FOR CONNECT")
def test_page_rank_2(examples, spark):
    n = 100
    g = _from_java_gf(getattr(examples, "star")(n), spark)
    resetProb = 0.15
    errorTol = 1.0e-5
    pr = g.pageRank(resetProb, tol=errorTol)
    _hasCols(pr, vcols=["id", "pagerank"], ecols=["src", "dst", "weight"])


@pytest.mark.skipif(is_remote(), reason="DISABLE FOR CONNECT")
def test_parallel_personalized_page_rank(examples, spark):
    n = 100
    g = _from_java_gf(getattr(examples, "star")(n), spark)
    resetProb = 0.15
    maxIter = 15
    sourceIds = [1, 2, 3, 4]
    pr = g.parallelPersonalizedPageRank(resetProb, sourceIds=sourceIds, maxIter=maxIter)
    _hasCols(pr, vcols=["id", "pageranks"], ecols=["src", "dst", "weight"])


def test_shortest_paths(spark):
    edges = [(1, 2), (1, 5), (2, 3), (2, 5), (3, 4), (4, 5), (4, 6)]
    # Create bidirectional edges.
    all_edges = [z for (a, b) in edges for z in [(a, b), (b, a)]]
    edges = spark.createDataFrame(all_edges, ["src", "dst"])
    edges = spark.createDataFrame(all_edges, ["src", "dst"])
    edgesDF = spark.createDataFrame(all_edges, ["src", "dst"])
    vertices = spark.createDataFrame([(i,) for i in range(1, 7)], ["id"])
    g = GraphFrame(vertices, edgesDF)
    landmarks = [1, 4]
    v2 = g.shortestPaths(landmarks)
    _df_hasCols(v2, vcols=["id", "distances"])


@pytest.mark.skipif(is_remote(), reason="DISABLE FOR CONNECT")
def test_svd_plus_plus(examples, spark):
    g = _from_java_gf(getattr(examples, "ALSSyntheticData")(), spark)
    (v2, cost) = g.svdPlusPlus()
    _df_hasCols(v2, vcols=["id", "column1", "column2", "column3", "column4"])


def test_strongly_connected_components(spark):
    # Simple island test
    vertices = spark.createDataFrame([(i,) for i in range(1, 6)], ["id"])
    edges = spark.createDataFrame([(7, 8)], ["src", "dst"])
    g = GraphFrame(vertices, edges)
    c = g.stronglyConnectedComponents(5)
    for row in c.collect():
        assert (
            row.id == row.component
        ), f"Vertex {row.id} not equal to its component {row.component}"


def test_triangle_counts(spark):
    edges = spark.createDataFrame([(0, 1), (1, 2), (2, 0)], ["src", "dst"])
    vertices = spark.createDataFrame([(0,), (1,), (2,)], ["id"])
    g = GraphFrame(vertices, edges)
    c = g.triangleCount()
    for row in c.select("id", "count").collect():
        assert row.asDict()["count"] == 1, f"Triangle count for vertex {row.id} is not 1"


@pytest.mark.skipif(is_remote(), reason="DISABLE FOR CONNECT")
def test_mutithreaded_sparksession_usage(spark):
    # Test that the GraphFrame API works correctly from multiple threads.
    localVertices = [(1, "A"), (2, "B"), (3, "C")]
    localEdges = [(1, 2, "love"), (2, 1, "hate"), (2, 3, "follow")]
    v = spark.createDataFrame(localVertices, ["id", "name"])
    e = spark.createDataFrame(localEdges, ["src", "dst", "action"])

    exc = None

    def run_graphframe() -> None:
        nonlocal exc
        try:
            GraphFrame(v, e)
        except Exception as _e:
            exc = _e

    import threading

    thread = threading.Thread(target=run_graphframe)
    thread.start()
    thread.join()
    assert exc is None, f"Exception was raised in thread: {exc}"


@pytest.mark.skipif(is_remote(), reason="DISABLE FOR CONNECT")
def test_belief_propagation(spark):
    # Create a graphical model g of size 3x3.
    g = Graphs(spark).gridIsingModel(3)
    # Run Belief Propagation (BP) for 5 iterations.
    numIter = 5
    results = BeliefPropagation.runBPwithGraphFrames(g, numIter)
    # Check that each belief is a valid probability in [0, 1].
    for row in results.vertices.select("belief").collect():
        belief = row["belief"]
        assert 0 <= belief <= 1, f"Expected belief to be probability in [0,1], but found {belief}"


@pytest.mark.skipif(is_remote(), reason="DISABLE FOR CONNECT")
def test_graph_friends(spark):
    # Construct the graph.
    g = Graphs(spark).friends()
    # Check that the result is an instance of GraphFrame.
    assert isinstance(g, GraphFrame)


@pytest.mark.skipif(is_remote(), reason="DISABLE FOR CONNECT")
def test_graph_grid_ising_model(spark):
    # Construct a grid Ising model graph.
    n = 3
    g = Graphs(spark).gridIsingModel(n)
    # Collect the vertex ids
    ids = [v["id"] for v in g.vertices.collect()]
    # Verify that every expected vertex id appears.
    for i in range(n):
        for j in range(n):
            assert f"{i},{j}" in ids


if __name__ == "__main__":
    pytest.main()
