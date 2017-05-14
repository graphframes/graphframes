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

import sys
import tempfile
import shutil

if sys.version_info[:2] <= (2, 6):
    try:
        import unittest2 as unittest
    except ImportError:
        sys.stderr.write('Please install unittest2 to test with Python 2.6 or earlier')
        sys.exit(1)
else:
    import unittest

from pyspark import SparkContext
from pyspark.sql import DataFrame, functions as sqlfunctions, SQLContext

from .graphframe import GraphFrame, _java_api, _from_java_gf
from .lib import AggregateMessages as AM
from .examples import Graphs, BeliefPropagation


class GraphFrameTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.sc = SparkContext('local[4]', cls.__name__)
        cls.checkpointDir = tempfile.mkdtemp()
        cls.sc.setCheckpointDir(cls.checkpointDir)
        cls.sql = SQLContext(cls.sc)
        # Small tests run much faster with spark.sql.shuffle.partitions=4
        cls.sql.setConf("spark.sql.shuffle.partitions", "4")

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()
        cls.sc = None
        cls.sql = None
        shutil.rmtree(cls.checkpointDir)


class GraphFrameTest(GraphFrameTestCase):
    def setUp(self):
        super(GraphFrameTest, self).setUp()
        localVertices = [(1, "A"), (2, "B"), (3, "C")]
        localEdges = [(1, 2, "love"), (2, 1, "hate"), (2, 3, "follow")]
        v = self.sql.createDataFrame(localVertices, ["id", "name"])
        e = self.sql.createDataFrame(localEdges, ["src", "dst", "action"])
        self.g = GraphFrame(v, e)

    def test_construction(self):
        g = self.g
        vertexIDs = map(lambda x: x[0], g.vertices.select("id").collect())
        assert sorted(vertexIDs) == [1, 2, 3]
        edgeActions = map(lambda x: x[0], g.edges.select("action").collect())
        assert sorted(edgeActions) == ["follow", "hate", "love"]
        tripletsFirst = list(map(lambda x: (x[0][1], x[1][1], x[2][2]),
                            g.triplets.sort("src.id").select("src", "dst", "edge").take(1)))
        assert tripletsFirst == [("A", "B", "love")], tripletsFirst
        # Try with invalid vertices and edges DataFrames
        v_invalid = self.sql.createDataFrame(
            [(1, "A"), (2, "B"), (3, "C")], ["invalid_colname_1", "invalid_colname_2"])
        e_invalid = self.sql.createDataFrame(
            [(1, 2), (2, 3), (3, 1)], ["invalid_colname_3", "invalid_colname_4"])
        with self.assertRaises(ValueError):
            GraphFrame(v_invalid, e_invalid)

    def test_cache(self):
        g = self.g
        g.cache()
        g.unpersist()

    def test_degrees(self):
        g = self.g
        outDeg = g.outDegrees
        self.assertSetEqual(set(outDeg.columns), {"id", "outDegree"})
        inDeg = g.inDegrees
        self.assertSetEqual(set(inDeg.columns), {"id", "inDegree"})
        deg = g.degrees
        self.assertSetEqual(set(deg.columns), {"id", "degree"})

    def test_motif_finding(self):
        g = self.g
        motifs = g.find("(a)-[e]->(b)")
        assert motifs.count() == 3
        self.assertSetEqual(set(motifs.columns), {"a", "e", "b"})

    def test_bfs(self):
        g = self.g
        paths = g.bfs("name='A'", "name='C'")
        self.assertEqual(paths.count(), 1)
        self.assertEqual(paths.select("v1.name").head()[0], "B")
        paths2 = g.bfs("name='A'", "name='C'", edgeFilter="action!='follow'")
        self.assertEqual(paths2.count(), 0)
        paths3 = g.bfs("name='A'", "name='C'", maxPathLength=1)
        self.assertEqual(paths3.count(), 0)


class GraphFrameLibTest(GraphFrameTestCase):
    def setUp(self):
        super(GraphFrameLibTest, self).setUp()
        self.sqlContext = self.sql
        self.japi = _java_api(self.sqlContext._sc)

    def _hasCols(self, graph, vcols = [], ecols = []):
        map(lambda c: self.assertIn(c, graph.vertices.columns), vcols)
        map(lambda c: self.assertIn(c, graph.edges.columns), ecols)

    def _df_hasCols(self, vertices, vcols = []):
        map(lambda c: self.assertIn(c, vertices.columns), vcols)

    def _graph(self, name, *args):
        """
        Convenience to call one of the example graphs, passing the arguments and wrapping the result back
        as a python object.
        :param name: the name of the example graph
        :param args: all the required arguments, without the initial sql context
        :return:
        """
        examples = self.japi.examples()
        jgraph = getattr(examples, name)(*args)
        return _from_java_gf(jgraph, self.sqlContext)

    def test_aggregate_messages(self):
        g = self._graph("friends")
        # For each user, sum the ages of the adjacent users,
        # plus 1 for the src's sum if the edge is "friend".
        sendToSrc = (
            AM.dst['age'] +
            sqlfunctions.when(
                AM.edge['relationship'] == 'friend',
                sqlfunctions.lit(1)
            ).otherwise(0))
        sendToDst = AM.src['age']
        agg = g.aggregateMessages(
            sqlfunctions.sum(AM.msg).alias('summedAges'),
            sendToSrc=sendToSrc,
            sendToDst=sendToDst)
        # Run the aggregation again providing SQL expressions as String instead.
        agg2 = g.aggregateMessages(
            "sum(MSG) AS `summedAges`",
            sendToSrc="(dst['age'] + CASE WHEN (edge['relationship'] = 'friend') THEN 1 ELSE 0 END)",
            sendToDst="src['age']")
        # Convert agg and agg2 to a mapping from id to the aggregated message.
        aggMap = {id_: s for id_, s in agg.select('id', 'summedAges').collect()}
        agg2Map = {id_: s for id_, s in agg2.select('id', 'summedAges').collect()}
        # Compute the truth via brute force.
        user2age = {id_: age for id_, age in g.vertices.select('id', 'age').collect()}
        trueAgg = {}
        for src, dst, rel in g.edges.select("src", "dst", "relationship").collect():
            trueAgg[src] = trueAgg.get(src, 0) + user2age[dst] + (1 if rel == 'friend' else 0)
            trueAgg[dst] = trueAgg.get(dst, 0) + user2age[src]
        # Compare if the agg mappings match the brute force mapping
        self.assertEqual(aggMap, trueAgg)
        self.assertEqual(agg2Map, trueAgg)
        # Check that TypeError is raises with messages of wrong type
        with self.assertRaises(TypeError):
            g.aggregateMessages(
                "sum(MSG) AS `summedAges`",
                sendToSrc=object(),
                sendToDst="src['age']")
        with self.assertRaises(TypeError):
            g.aggregateMessages(
                "sum(MSG) AS `summedAges`",
                sendToSrc=dst['age'],
                sendToDst=object())

    def test_connected_components(self):
        v = self.sqlContext.createDataFrame([
        (0, "a", "b")], ["id", "vattr", "gender"])
        e = self.sqlContext.createDataFrame([(0, 0, 1)], ["src", "dst", "test"]).filter("src > 10")
        g = GraphFrame(v, e)
        comps = g.connectedComponents()
        self._df_hasCols(comps, vcols=['id', 'component', 'vattr', 'gender'])
        self.assertEqual(comps.count(), 1)

    def test_connected_components2(self):
        v = self.sqlContext.createDataFrame([(0, "a0", "b0"), (1, "a1", "b1")], ["id", "A", "B"])
        e = self.sqlContext.createDataFrame([(0, 1, "a01", "b01")], ["src", "dst", "A", "B"])
        g = GraphFrame(v, e)
        comps = g.connectedComponents()
        self._df_hasCols(comps, vcols=['id', 'component', 'A', 'B'])
        self.assertEqual(comps.count(), 2)

    def test_connected_components_friends(self):
        g = self._graph("friends")
        comps0 = g.connectedComponents()
        comps1 = g.connectedComponents(broadcastThreshold=1)
        comps2 = g.connectedComponents(checkpointInterval=0)
        comps3 = g.connectedComponents(checkpointInterval=10)
        compsX = g.connectedComponents(algorithm="graphx")
        for c in [comps0, comps1, comps2, comps3, compsX]:
            self.assertEqual(c.groupBy("component").count().count(), 2)

    def test_label_progagation(self):
        n = 5
        g = self._graph("twoBlobs", n)
        labels = g.labelPropagation(maxIter=4 * n)
        labels1 = labels.filter("id < 5").select("label").collect()
        all1 = set([x.label for x in labels1])
        self.assertEqual(all1, set([0]))
        labels2 = labels.filter("id >= 5").select("label").collect()
        all2 = set([x.label for x in labels2])
        self.assertEqual(all2, set([n]))

    def test_page_rank(self):
        n = 100
        g = self._graph("star", n)
        resetProb = 0.15
        errorTol = 1.0e-5
        pr = g.pageRank(resetProb, tol=errorTol)
        self._hasCols(pr, vcols=['id', 'pagerank'], ecols=['src', 'dst', 'weight'])

    def test_shortest_paths(self):
        edges = [(1, 2), (1, 5), (2, 3), (2, 5), (3, 4), (4, 5), (4, 6)]
        all_edges = [z for (a, b) in edges for z in [(a, b), (b, a)]]
        edges = self.sqlContext.createDataFrame(all_edges, ["src", "dst"])
        vertices = self.sqlContext.createDataFrame([(i,) for i in range(1, 7)], ["id"])
        g = GraphFrame(vertices, edges)
        landmarks = [1, 4]
        v2 = g.shortestPaths(landmarks)
        self._df_hasCols(v2, vcols=["id", "distances"])

    def test_svd_plus_plus(self):
        g = self._graph("ALSSyntheticData")
        (v2, cost) = g.svdPlusPlus()
        self._df_hasCols(v2, vcols=['id', 'column1', 'column2', 'column3', 'column4'])

    def test_strongly_connected_components(self):
        # Simple island test
        vertices = self.sqlContext.createDataFrame([(i,) for i in range(1, 6)], ["id"])
        edges = self.sqlContext.createDataFrame([(7, 8)], ["src", "dst"])
        g = GraphFrame(vertices, edges)
        c = g.stronglyConnectedComponents(5)
        for row in c.collect():
            self.assertEqual(row.id, row.component)

    def test_triangle_counts(self):
        edges = self.sqlContext.createDataFrame([(0, 1), (1, 2), (2, 0)], ["src", "dst"])
        vertices = self.sqlContext.createDataFrame([(0,), (1,), (2,)], ["id"])
        g = GraphFrame(vertices, edges)
        c = g.triangleCount()
        for row in c.select("id", "count").collect():
            self.assertEqual(row.asDict()['count'], 1)


class GraphFrameExamplesTest(GraphFrameTestCase):
    def setUp(self):
        super(GraphFrameExamplesTest, self).setUp()
        self.sqlContext = self.sql
        self.japi = _java_api(self.sqlContext._sc)

    def test_belief_propagation(self):
        # create graphical model g of size 3 x 3
        g = Graphs(self.sqlContext).gridIsingModel(3)
        # run BP for 5 iterations
        numIter = 5
        results = BeliefPropagation.runBPwithGraphFrames(g, numIter)
        # check beliefs are valid
        for row in results.vertices.select('belief').collect():
            belief = row['belief']
            self.assertTrue(
                0 <= belief <= 1,
                msg="Expected belief to be probability in [0,1], but found {}".format(belief))

    def test_graph_friends(self):
        # construct graph
        g = Graphs(self.sqlContext).friends()
        # check that a GraphFrame instance was returned
        self.assertIsInstance(g, GraphFrame)

    def test_graph_grid_ising_model(self):
        # construct graph
        n = 3
        g = Graphs(self.sqlContext).gridIsingModel(n)
        # check that all the vertices exist
        ids = [v['id'] for v in g.vertices.collect()]
        for i in range(n):
            for j in range(n):
                self.assertIn('{},{}'.format(i, j), ids)
