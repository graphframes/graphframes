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


from dataclasses import dataclass
from pyspark.storagelevel import StorageLevel
import pytest
from pyspark.sql import DataFrame, SparkSession, functions as sqlfunctions

from graphframes.classic.graphframe import _from_java_gf
from graphframes.examples import BeliefPropagation, Graphs
from graphframes.graphframe import GraphFrame


@dataclass
class PregelArguments:
    algorithm: str
    use_local_checkpoints: bool
    checkpoint_interval: int
    storage_level: StorageLevel


PREGEL_ARGUMENTS = [
    PregelArguments("graphframes", True, 5, StorageLevel.MEMORY_AND_DISK),
    PregelArguments("graphx", False, 3, StorageLevel.DISK_ONLY),
    PregelArguments("graphframes", False, 7, StorageLevel.MEMORY_ONLY),
    PregelArguments("graphframes", True, 1, StorageLevel.DISK_ONLY_3),
]
PREGEL_IDS: list[str] = [
    "graphframes,local,5,MEMORY_AND_DISK",
    "graphx,global,3,DISK_ONLY",
    "graphframes,global,7,MEMORY_ONLY",
    "graphframes,local,1,DISK_ONLY_3",
]
STORAGE_LEVELS = [
    StorageLevel.MEMORY_AND_DISK_2,
    StorageLevel.DISK_ONLY,
    StorageLevel.MEMORY_ONLY,
]
STORAGE_LEVELS_IDS = [
    "MEMORY_AND_DISK_2",
    "DISK_ONLY",
    "MEMORY_ONLY",
]


def test_construction(spark: SparkSession, local_g: GraphFrame) -> None:
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
        _ = GraphFrame(v_invalid, e_invalid)


def test_validate(spark: SparkSession) -> None:
    good_g = GraphFrame(
        spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")]).toDF("id", "attr"),
        spark.createDataFrame([(1, 2), (2, 1), (2, 3)]).toDF("src", "dst"),
    )
    good_g.validate()  # no exception should be thrown

    not_distinct_vertices = GraphFrame(
        spark.createDataFrame([(1, "a"), (2, "b"), (3, "c"), (1, "d")]).toDF(
            "id", "attr"
        ),
        spark.createDataFrame([(1, 2), (2, 1), (2, 3)]).toDF("src", "dst"),
    )
    with pytest.raises(ValueError):
        not_distinct_vertices.validate()

    missing_vertices = GraphFrame(
        spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")]).toDF("id", "attr"),
        spark.createDataFrame([(1, 2), (2, 1), (2, 3), (1, 4)]).toDF("src", "dst"),
    )
    with pytest.raises(ValueError):
        missing_vertices.validate()


def test_as_undirected(spark: SparkSession) -> None:
    # Test without edge attributes
    v = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")]).toDF("id", "name")
    e = spark.createDataFrame([(1, 2), (2, 3)]).toDF("src", "dst")
    g = GraphFrame(v, e)
    undirected = g.as_undirected()

    # Check edge count doubled
    assert undirected.edges.count() == 2 * g.edges.count()

    # Verify reverse edges exist
    edges = undirected.edges.sort("src", "dst").collect()
    assert len(edges) == 4
    assert edges[0][0] == 1
    assert edges[0][1] == 2
    assert edges[1][0] == 2
    assert edges[1][1] == 1
    assert edges[2][0] == 2
    assert edges[2][1] == 3
    assert edges[3][0] == 3
    assert edges[3][1] == 2

    # Test with edge attributes
    v2 = spark.createDataFrame([(1, "a"), (2, "b")]).toDF("id", "name")
    e2 = spark.createDataFrame([(1, 2, "edge1")]).toDF("src", "dst", "attr")
    g2 = GraphFrame(v2, e2)
    undirected2 = g2.as_undirected()

    edges2 = undirected2.edges.collect()
    assert len(edges2) == 2
    assert any(row[0] == 1 and row[1] == 2 and row[2] == "edge1" for row in edges2)
    assert any(row[0] == 2 and row[1] == 1 and row[2] == "edge1" for row in edges2)


def test_cache(local_g: GraphFrame) -> None:
    _ = local_g.cache()
    _ = local_g.unpersist()


def test_degrees(local_g: GraphFrame) -> None:
    outDeg = local_g.outDegrees
    assert set(outDeg.columns) == {"id", "outDegree"}
    inDeg = local_g.inDegrees
    assert set(inDeg.columns) == {"id", "inDegree"}
    deg = local_g.degrees
    assert set(deg.columns) == {"id", "degree"}


def test_motif_finding(local_g: GraphFrame) -> None:
    motifs = local_g.find("(a)-[e]->(b)")
    assert motifs.count() == 3
    assert set(motifs.columns) == {"a", "e", "b"}


def test_filterVertices(local_g: GraphFrame) -> None:
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


def test_filterEdges(local_g: GraphFrame) -> None:
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


def test_dropIsolatedVertices(local_g: GraphFrame) -> None:
    g2 = local_g.filterEdges("dst > 2").dropIsolatedVertices()
    v2 = g2.vertices.select("id", "name").collect()
    e2 = g2.edges.select("src", "dst", "action").collect()
    expected_v = [(2, "B"), (3, "C")]
    expected_e = [(2, 3, "follow")]
    assert len(v2) == len(expected_v)
    assert len(e2) == len(expected_e)
    assert set(v2) == set(expected_v)
    assert set(e2) == set(expected_e)


def test_bfs(local_g: GraphFrame) -> None:
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


def test_power_iteration_clustering(spark: SparkSession) -> None:
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
    clusters_df = g.powerIterationClustering(k=2, maxIter=40, weightCol="weight")

    clusters = [r["cluster"] for r in clusters_df.sort("id").collect()]

    assert clusters == [0, 0, 0, 0, 1, 0]
    _ = clusters_df.unpersist()


@pytest.mark.parametrize("args", PREGEL_ARGUMENTS, ids=PREGEL_IDS)
def test_page_rank(spark: SparkSession, args: PregelArguments) -> None:
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
    _ = edges.cache()
    vertices = spark.createDataFrame([[0], [1], [2], [3], [4]], ["id"])
    numVertices = vertices.count()

    vertices = GraphFrame(vertices, edges).outDegrees
    _ = vertices.toPandas().head()
    _ = vertices.cache()

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
    _ = ranks.unpersist()


@pytest.mark.parametrize("args", PREGEL_ARGUMENTS, ids=PREGEL_IDS)
def test_pregel_early_stopping(spark: SparkSession, args: PregelArguments) -> None:
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
    _ = edges.cache()
    vertices = spark.createDataFrame([[0], [1], [2], [3], [4]], ["id"])
    numVertices = vertices.count()

    vertices = GraphFrame(vertices, edges).outDegrees
    _ = vertices.toPandas().head()
    _ = vertices.cache()

    # Construct a new GraphFrame with the updated vertices DataFrame.
    graph = GraphFrame(vertices, edges)
    alpha = 0.15
    pregel = graph.pregel
    ranks = (
        graph.pregel.setMaxIter(5)
        .setUseLocalCheckpoints(args.use_local_checkpoints)
        .setIntermediateStorageLevel(args.storage_level)
        .setCheckpointInterval(args.checkpoint_interval)
        .setEarlyStopping(True)
        .setUseLocalCheckpoints(args.use_local_checkpoints)
        .setIntermediateStorageLevel(args.storage_level)
        .setCheckpointInterval(args.checkpoint_interval)
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
    _ = ranks.unpersist()


def _hasCols(graph: GraphFrame, vcols: list[str] = [], ecols: list[str] = []) -> None:
    for c in vcols:
        assert c in graph.vertices.columns, f"Vertex DataFrame missing column: {c}"
    for c in ecols:
        assert c in graph.edges.columns, f"Edge DataFrame missing column: {c}"


def _df_hasCols(df: DataFrame, vcols: list[str] = []) -> None:
    for c in vcols:
        assert c in df.columns, f"DataFrame missing column: {c}"


@pytest.mark.parametrize("args", PREGEL_ARGUMENTS, ids=PREGEL_IDS)
@pytest.mark.parametrize(
    "cc_args",
    [(-1, True), (10000, True), (-1, False), (10000, False)],
    ids=["aqe,local", "skewed,local", "aqe,checkpoints", "skewed,checkpoints"],
)
def test_connected_components(
    spark: SparkSession, args: PregelArguments, cc_args: tuple[int, bool]
) -> None:
    v = spark.createDataFrame([(0, "a", "b")], ["id", "vattr", "gender"])
    e = spark.createDataFrame([(0, 0, 1)], ["src", "dst", "test"]).filter("src > 10")
    v = spark.createDataFrame([(0, "a", "b")], ["id", "vattr", "gender"])
    e = spark.createDataFrame([(0, 0, 1)], ["src", "dst", "test"]).filter("src > 10")
    g = GraphFrame(v, e)
    comps = g.connectedComponents(
        algorithm=args.algorithm,
        checkpointInterval=args.checkpoint_interval,
        use_local_checkpoints=args.use_local_checkpoints,
        storage_level=args.storage_level,
        broadcastThreshold=cc_args[0],
        useLabelsAsComponents=cc_args[1],
    )
    _df_hasCols(comps, vcols=["id", "component", "vattr", "gender"])
    assert comps.count() == 1
    _ = comps.unpersist()


@pytest.mark.parametrize("args", PREGEL_ARGUMENTS, ids=PREGEL_IDS)
@pytest.mark.parametrize(
    "cc_args",
    [(-1, True), (10000, True), (-1, False), (10000, False)],
    ids=["aqe,local", "skewed,local", "aqe,checkpoints", "skewed,checkpoints"],
)
def test_connected_components2(
    spark: SparkSession, args: PregelArguments, cc_args: tuple[int, bool]
) -> None:
    v = spark.createDataFrame([(0, "a0", "b0"), (1, "a1", "b1")], ["id", "A", "B"])
    e = spark.createDataFrame([(0, 1, "a01", "b01")], ["src", "dst", "A", "B"])
    g = GraphFrame(v, e)
    comps = g.connectedComponents(
        algorithm=args.algorithm,
        checkpointInterval=args.checkpoint_interval,
        use_local_checkpoints=args.use_local_checkpoints,
        storage_level=args.storage_level,
        broadcastThreshold=cc_args[0],
        useLabelsAsComponents=cc_args[1],
    )
    _df_hasCols(comps, vcols=["id", "component", "A", "B"])
    assert comps.count() == 2
    _ = comps.unpersist()


@pytest.mark.parametrize("args", PREGEL_ARGUMENTS, ids=PREGEL_IDS)
def test_shortest_paths(spark: SparkSession, args: PregelArguments) -> None:
    edges = [(1, 2), (1, 5), (2, 3), (2, 5), (3, 4), (4, 5), (4, 6)]
    # Create bidirectional edges.
    all_edges = [z for (a, b) in edges for z in [(a, b), (b, a)]]
    edges = spark.createDataFrame(all_edges, ["src", "dst"])
    edges = spark.createDataFrame(all_edges, ["src", "dst"])
    edgesDF = spark.createDataFrame(all_edges, ["src", "dst"])
    vertices = spark.createDataFrame([(i,) for i in range(1, 7)], ["id"])
    g = GraphFrame(vertices, edgesDF)
    landmarks: list[str | int] = [1, 4]
    v2 = g.shortestPaths(
        landmarks=landmarks,
        algorithm=args.algorithm,
        use_local_checkpoints=args.use_local_checkpoints,
        checkpoint_interval=args.checkpoint_interval,
        storage_level=args.storage_level,
    )
    _df_hasCols(v2, vcols=["id", "distances"])
    _ = v2.unpersist()


def test_strongly_connected_components(spark: SparkSession) -> None:
    # Simple island test
    vertices = spark.createDataFrame([(i,) for i in range(1, 6)], ["id"])
    edges = spark.createDataFrame([(7, 8)], ["src", "dst"])
    g = GraphFrame(vertices, edges)
    c = g.stronglyConnectedComponents(5)
    for row in c.collect():
        assert row.id == row.component, (
            f"Vertex {row.id} not equal to its component {row.component}"
        )
    _ = c.unpersist()


@pytest.mark.parametrize("storage_level", STORAGE_LEVELS, ids=STORAGE_LEVELS_IDS)
def test_triangle_counts(spark: SparkSession, storage_level: StorageLevel) -> None:
    edges = spark.createDataFrame([(0, 1), (1, 2), (2, 0)], ["src", "dst"])
    vertices = spark.createDataFrame([(0,), (1,), (2,)], ["id"])
    g = GraphFrame(vertices, edges)
    c = g.triangleCount(storage_level=storage_level)
    for row in c.select("id", "count").collect():
        assert row.asDict()["count"] == 1, (
            f"Triangle count for vertex {row.id} is not 1"
        )
    _ = c.unpersist()


@pytest.mark.parametrize("args", PREGEL_ARGUMENTS, ids=PREGEL_IDS)
def test_cycles_finding(spark: SparkSession, args: PregelArguments) -> None:
    vertices = spark.createDataFrame(
        [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")], ["id", "attr"]
    )
    edges = spark.createDataFrame(
        [(1, 2), (2, 3), (3, 1), (1, 4), (2, 5)], ["src", "dst"]
    )
    graph = GraphFrame(vertices, edges)
    res = graph.detectingCycles(
        checkpoint_interval=args.checkpoint_interval,
        use_local_checkpoints=args.use_local_checkpoints,
        storage_level=args.storage_level,
    )
    assert res.count() == 3
    collected = res.sort("id").select("found_cycles").collect()
    assert [row[0] for row in collected] == [[1, 2, 3, 1], [2, 3, 1, 2], [3, 1, 2, 3]]
    _ = res.unpersist()
