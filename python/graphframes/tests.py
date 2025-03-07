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

import re
import shutil
import tempfile

import pytest
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as sqlfunctions

from .examples import BeliefPropagation, Graphs
from .graphframe import GraphFrame, Pregel, _from_java_gf, _java_api
from .lib import AggregateMessages as AM


@pytest.fixture(scope="class", autouse=True)
def set_spark(request, spark_session):
    request.cls.spark = spark_session


@pytest.mark.usefixtures("set_spark")
class GraphFrameTestUtils(object):
    @classmethod
    def parse_spark_version(cls, version_str):
        """take an input version string
        return version items in a dictionary
        """
        _sc_ver_patt = r"(\d+)\.(\d+)(\.(\d+)(-(.+))?)?"
        m = re.match(_sc_ver_patt, version_str)
        if not m:
            raise TypeError(
                "version {} shoud be in <major>.<minor>.<maintenance>".format(version_str)
            )
        version_info = {}
        try:
            version_info["major"] = int(m.group(1))
        except:  # noqa: E722
            raise TypeError("invalid minor version")
        try:
            version_info["minor"] = int(m.group(2))
        except:  # noqa: E722
            raise TypeError("invalid major version")
        try:
            version_info["maintenance"] = int(m.group(4))
        except:  # noqa: E722
            version_info["maintenance"] = 0
        try:
            version_info["special"] = m.group(6)
        except:  # noqa: E722
            pass
        return version_info

    @classmethod
    def createSparkContext(cls):
        cls.sc = SparkContext("local[4]", "GraphFramesTests")
        cls.checkpointDir = tempfile.mkdtemp()
        cls.sc.setCheckpointDir(cls.checkpointDir)
        cls.spark_version = cls.parse_spark_version(cls.sc.version)

    @classmethod
    def stopSparkContext(cls):
        cls.sc.stop()
        cls.sc = None
        shutil.rmtree(cls.checkpointDir)

    @classmethod
    def spark_at_least_of_version(cls, version_str):
        assert hasattr(cls, "spark_version")
        required_version = cls.parse_spark_version(version_str)
        spark_version = cls.spark_version
        for _name in ["major", "minor", "maintenance"]:
            sc_ver = spark_version[_name]
            req_ver = required_version[_name]
            if sc_ver != req_ver:
                return sc_ver > req_ver
        # All major.minor.maintenance equal
        return True


@pytest.fixture(scope="module", autouse=True)
def spark_context():
    GraphFrameTestUtils.createSparkContext()
    yield
    GraphFrameTestUtils.stopSparkContext()


@pytest.fixture(scope="class")
def spark_session():
    # Create a SparkSession with a smaller number of shuffle partitions.
    spark = (
        SparkSession(GraphFrameTestUtils.sc)
        .builder.config("spark.sql.shuffle.partitions", 4)
        .getOrCreate()
    )
    yield spark
    # No explicit stop; SparkContext shutdown will clean up.


@pytest.mark.usefixtures("set_spark")
class GraphFrameTest:
    def setup_method(self, method):
        # Mimic setUp: create a simple GraphFrame instance for each test.
        localVertices = [(1, "A"), (2, "B"), (3, "C")]
        localEdges = [(1, 2, "love"), (2, 1, "hate"), (2, 3, "follow")]
        v = self.spark.createDataFrame(localVertices, ["id", "name"])
        e = self.spark.createDataFrame(localEdges, ["src", "dst", "action"])
        self.g = GraphFrame(v, e)

    def test_spark_version_check(self):
        gtu = GraphFrameTestUtils
        gtu.spark_version = gtu.parse_spark_version("2.0.2")

        assert gtu.spark_at_least_of_version("1.7")
        assert gtu.spark_at_least_of_version("2.0")
        assert gtu.spark_at_least_of_version("2.0.1")
        assert gtu.spark_at_least_of_version("2.0.2")
        assert not gtu.spark_at_least_of_version("2.0.3")
        assert not gtu.spark_at_least_of_version("2.1")

    def test_construction(self):
        g = self.g
        vertexIDs = [row[0] for row in g.vertices.select("id").collect()]
        assert sorted(vertexIDs) == [1, 2, 3]

        edgeActions = [row[0] for row in g.edges.select("action").collect()]
        assert sorted(edgeActions) == ["follow", "hate", "love"]
        tripletsFirst = list(
            map(
                lambda x: (x[0][1], x[1][1], x[2][2]),
                g.triplets.sort("src.id").select("src", "dst", "edge").take(1),
            )
        )
        assert tripletsFirst == [("A", "B", "love")], tripletsFirst

        # Try with invalid vertices and edges DataFrames
        v_invalid = self.spark.createDataFrame(
            [(1, "A"), (2, "B"), (3, "C")], ["invalid_colname_1", "invalid_colname_2"]
        )
        e_invalid = self.spark.createDataFrame(
            [(1, 2), (2, 3), (3, 1)], ["invalid_colname_3", "invalid_colname_4"]
        )

        with pytest.raises(ValueError):
            GraphFrame(v_invalid, e_invalid)

    def test_cache(self):
        g = self.g
        g.cache()
        g.unpersist()

    def test_degrees(self):
        g = self.g
        outDeg = g.outDegrees
        assert set(outDeg.columns) == {"id", "outDegree"}
        inDeg = g.inDegrees
        assert set(inDeg.columns) == {"id", "inDegree"}
        deg = g.degrees
        assert set(deg.columns) == {"id", "degree"}

    def test_motif_finding(self):
        g = self.g
        motifs = g.find("(a)-[e]->(b)")
        assert motifs.count() == 3
        assert set(motifs.columns) == {"a", "e", "b"}

    def test_filterVertices(self):
        g = self.g
        conditions = ["id < 3", g.vertices.id < 3]
        expected_v = [(1, "A"), (2, "B")]
        expected_e = [(1, 2, "love"), (2, 1, "hate")]
        for cond in conditions:
            g2 = g.filterVertices(cond)
            v2 = g2.vertices.select("id", "name").collect()
            e2 = g2.edges.select("src", "dst", "action").collect()
            assert len(v2) == len(expected_v)
            assert len(e2) == len(expected_e)
            assert set(v2) == set(expected_v)
            assert set(e2) == set(expected_e)

    def test_filterEdges(self):
        g = self.g
        conditions = ["dst > 2", g.edges.dst > 2]
        expected_v = [(1, "A"), (2, "B"), (3, "C")]
        expected_e = [(2, 3, "follow")]
        for cond in conditions:
            g2 = g.filterEdges(cond)
            v2 = g2.vertices.select("id", "name").collect()
            e2 = g2.edges.select("src", "dst", "action").collect()
            assert len(v2) == len(expected_v)
            assert len(e2) == len(expected_e)
            assert set(v2) == set(expected_v)
            assert set(e2) == set(expected_e)

    def test_dropIsolatedVertices(self):
        g = self.g
        g2 = g.filterEdges("dst > 2").dropIsolatedVertices()
        v2 = g2.vertices.select("id", "name").collect()
        e2 = g2.edges.select("src", "dst", "action").collect()
        expected_v = [(2, "B"), (3, "C")]
        expected_e = [(2, 3, "follow")]
        assert len(v2) == len(expected_v)
        assert len(e2) == len(expected_e)
        assert set(v2) == set(expected_v)
        assert set(e2) == set(expected_e)

    def test_bfs(self):
        g = self.g
        paths = g.bfs("name='A'", "name='C'")
        assert paths.count() == 1
        # Expecting that the first intermediary vertex in the BFS is "B"
        assert paths.select("v1.name").head()[0] == "B"

        paths2 = g.bfs("name='A'", "name='C'", edgeFilter="action!='follow'")
        assert paths2.count() == 0

        paths3 = g.bfs("name='A'", "name='C'", maxPathLength=1)
        assert paths3.count() == 0


@pytest.mark.usefixtures("set_spark")
class TestPregel:
    def test_page_rank(self):
        # Create an edge DataFrame; note that vertex 3 has no in-links.
        from pyspark.sql.functions import coalesce, lit, sum

        edges = self.spark.createDataFrame(
            [
                [0, 1],
                [1, 2],
                [2, 4],
                [2, 0],
                [3, 4],
                [4, 0],
                [4, 2],
            ],  # 3 has no in-links
            ["src", "dst"],
        )
        edges.cache()

        # Create a vertex DataFrame and count vertices.
        vertices = self.spark.createDataFrame([[0], [1], [2], [3], [4]], ["id"])
        numVertices = vertices.count()

        # Get the outDegrees DataFrame from a GraphFrame built on the original vertices and edges.
        vertices = GraphFrame(vertices, edges).outDegrees
        vertices.cache()

        # Construct a new GraphFrame with the updated vertices DataFrame.
        graph = GraphFrame(vertices, edges)
        alpha = 0.15

        pregel = graph.pregel
        ranks = (
            pregel.setMaxIter(5)
            .withVertexColumn(
                "rank",
                lit(1.0 / numVertices),
                coalesce(pregel.msg(), lit(0.0)) * lit(1.0 - alpha)
                + lit(alpha / numVertices),
            )
            .sendMsgToDst(pregel.src("rank") / pregel.src("outDegree"))
            .aggMsgs(sum(pregel.msg()))
            .run()
        )

        # Collect and sort results.
        resultRows = ranks.sort(ranks.id).collect()
        result = list(map(lambda x: x.rank, resultRows))
        expected = [0.245, 0.224, 0.303, 0.03, 0.197]

        # Compare each result with its expected value using a tolerance of 1e-3.
        for a, b in zip(result, expected):
            assert a == pytest.approx(b, abs=1e-3)


@pytest.mark.usefixtures("set_spark")
class TestGraphFrameLib:
    def setup_method(self, method):
        # Set up the Java API instance for each test.
        self.japi = _java_api(self.spark._sc)

    def _hasCols(self, graph, vcols=[], ecols=[]):
        for c in vcols:
            assert c in graph.vertices.columns, f"Vertex DataFrame missing column: {c}"
        for c in ecols:
            assert c in graph.edges.columns, f"Edge DataFrame missing column: {c}"

    def _df_hasCols(self, df, vcols=[]):
        for c in vcols:
            assert c in df.columns, f"DataFrame missing column: {c}"

    def _graph(self, name, *args):
        """
        Convenience to call one of the example graphs, passing the arguments and wrapping the result
        as a Python object.
        :param name: the name of the example graph.
        :param args: all the required arguments (excluding the initial SparkSession).
        :return: a GraphFrame object.
        """

        examples = self.japi.examples()
        jgraph = getattr(examples, name)(*args)
        return _from_java_gf(jgraph, self.spark)

    def test_aggregate_messages(self):
        g = self._graph("friends")
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
            g.aggregateMessages(
                "sum(MSG) AS `summedAges`", sendToSrc=object(), sendToDst="src['age']"
            )
        with pytest.raises(TypeError):
            g.aggregateMessages(
                "sum(MSG) AS `summedAges`", sendToSrc=dst["age"], sendToDst=object()
            )

    def test_connected_components(self):
        v = self.spark.createDataFrame([(0, "a", "b")], ["id", "vattr", "gender"])
        e = self.spark.createDataFrame([(0, 0, 1)], ["src", "dst", "test"]).filter("src > 10")
        g = GraphFrame(v, e)
        comps = g.connectedComponents()
        self._df_hasCols(comps, vcols=["id", "component", "vattr", "gender"])
        assert comps.count() == 1

    def test_connected_components2(self):
        v = self.spark.createDataFrame([(0, "a0", "b0"), (1, "a1", "b1")], ["id", "A", "B"])
        e = self.spark.createDataFrame([(0, 1, "a01", "b01")], ["src", "dst", "A", "B"])
        g = GraphFrame(v, e)
        comps = g.connectedComponents()
        self._df_hasCols(comps, vcols=["id", "component", "A", "B"])
        assert comps.count() == 2

    def test_connected_components_friends(self):
        g = self._graph("friends")
        comps_tests = [
            g.connectedComponents(),
            g.connectedComponents(broadcastThreshold=1),
            g.connectedComponents(checkpointInterval=0),
            g.connectedComponents(checkpointInterval=10),
            g.connectedComponents(algorithm="graphx"),
        ]
        for c in comps_tests:
            assert c.groupBy("component").count().count() == 2

    def test_label_progagation(self):
        n = 5
        g = self._graph("twoBlobs", n)
        labels = g.labelPropagation(maxIter=4 * n)
        labels1 = labels.filter("id < 5").select("label").collect()
        all1 = {row.label for row in labels1}
        assert len(all1) == 1
        labels2 = labels.filter("id >= 5").select("label").collect()
        all2 = {row.label for row in labels2}
        assert len(all2) == 1
        assert all1 != all2

    def test_page_rank(self):
        n = 100
        g = self._graph("star", n)
        resetProb = 0.15
        errorTol = 1.0e-5
        pr = g.pageRank(resetProb, tol=errorTol)
        self._hasCols(pr, vcols=["id", "pagerank"], ecols=["src", "dst", "weight"])

    def test_parallel_personalized_page_rank(self):
        n = 100
        g = self._graph("star", n)
        resetProb = 0.15
        maxIter = 15
        sourceIds = [1, 2, 3, 4]
        pr = g.parallelPersonalizedPageRank(resetProb, sourceIds=sourceIds, maxIter=maxIter)
        self._hasCols(pr, vcols=["id", "pageranks"], ecols=["src", "dst", "weight"])

    def test_shortest_paths(self):
        edges = [(1, 2), (1, 5), (2, 3), (2, 5), (3, 4), (4, 5), (4, 6)]
        # Create bidirectional edges.
        all_edges = [z for (a, b) in edges for z in [(a, b), (b, a)]]
        edgesDF = self.spark.createDataFrame(all_edges, ["src", "dst"])
        vertices = self.spark.createDataFrame([(i,) for i in range(1, 7)], ["id"])
        g = GraphFrame(vertices, edgesDF)
        landmarks = [1, 4]
        v2 = g.shortestPaths(landmarks)
        self._df_hasCols(v2, vcols=["id", "distances"])

    def test_svd_plus_plus(self):
        g = self._graph("ALSSyntheticData")
        (v2, cost) = g.svdPlusPlus()
        self._df_hasCols(v2, vcols=["id", "column1", "column2", "column3", "column4"])

    def test_strongly_connected_components(self):
        # Simple island test.
        vertices = self.spark.createDataFrame([(i,) for i in range(1, 6)], ["id"])
        edges = self.spark.createDataFrame([(7, 8)], ["src", "dst"])
        g = GraphFrame(vertices, edges)
        c = g.stronglyConnectedComponents(5)
        for row in c.collect():
            assert (
                row.id == row.component
            ), f"Vertex {row.id} not equal to its component {row.component}"

    def test_triangle_counts(self):
        edges = self.spark.createDataFrame([(0, 1), (1, 2), (2, 0)], ["src", "dst"])
        vertices = self.spark.createDataFrame([(0,), (1,), (2,)], ["id"])
        g = GraphFrame(vertices, edges)
        c = g.triangleCount()
        for row in c.select("id", "count").collect():
            assert row.asDict()["count"] == 1, f"Triangle count for vertex {row.id} is not 1"

    def test_mutithreaded_sparksession_usage(self):
        # Test that the GraphFrame API works correctly from multiple threads.
        localVertices = [(1, "A"), (2, "B"), (3, "C")]
        localEdges = [(1, 2, "love"), (2, 1, "hate"), (2, 3, "follow")]
        v = self.spark.createDataFrame(localVertices, ["id", "name"])
        e = self.spark.createDataFrame(localEdges, ["src", "dst", "action"])

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


@pytest.mark.usefixtures("set_spark")
class TestGraphFrameExamples:
    def setup_method(self, method):
        # Set up the Java API instance for use in the tests.
        self.japi = _java_api(self.spark._sc)

    def test_belief_propagation(self):
        # Create a graphical model g of size 3x3.
        g = Graphs(self.spark).gridIsingModel(3)
        # Run Belief Propagation (BP) for 5 iterations.
        numIter = 5
        results = BeliefPropagation.runBPwithGraphFrames(g, numIter)

        # Check that each belief is a valid probability in [0, 1].
        for row in results.vertices.select("belief").collect():
            belief = row["belief"]
            assert (
                0 <= belief <= 1
            ), f"Expected belief to be probability in [0,1], but found {belief}"

    def test_graph_friends(self):
        # Construct the graph.
        g = Graphs(self.spark).friends()
        # Check that the result is an instance of GraphFrame.
        assert isinstance(g, GraphFrame)

    def test_graph_grid_ising_model(self):
        # Construct a grid Ising model graph.
        n = 3
        g = Graphs(self.spark).gridIsingModel(n)
        # Collect the vertex ids
        ids = [v["id"] for v in g.vertices.collect()]
        # Verify that every expected vertex id appears.
        for i in range(n):
            for j in range(n):
                expected_id = f"{i},{j}"
                assert expected_id in ids, f"Vertex {expected_id} not found in {ids}"
                assert expected_id in ids, f"Vertex {expected_id} not found in {ids}"
