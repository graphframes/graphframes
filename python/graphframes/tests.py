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

if sys.version_info[:2] <= (2, 6):
    try:
        import unittest2 as unittest
    except ImportError:
        sys.stderr.write('Please install unittest2 to test with Python 2.6 or earlier')
        sys.exit(1)
else:
    import unittest

from pyspark import SparkContext
from pyspark.sql import DataFrame, SQLContext

from .graphframe import GraphFrame, _java_api, _from_java_gf


class GraphFrameTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.sc = SparkContext('local[4]', cls.__name__)
        cls.sql = SQLContext(cls.sc)
        # Small tests run much faster with spark.sql.shuffle.partitions=4
        cls.sql.setConf("spark.sql.shuffle.partitions", "4")

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()
        cls.sc = None
        cls.sql = None


class GraphFrameTest(GraphFrameTestCase):
    def setUp(self):
        super(GraphFrameTest, self).setUp()
        localVertices = [(1,"A"), (2,"B"), (3, "C")]
        localEdges = [(1,2,"love"), (2,1,"hate"), (2,3,"follow")]
        v = self.sql.createDataFrame(localVertices, ["id", "name"])
        e = self.sql.createDataFrame(localEdges, ["src", "dst", "action"])
        self.g = GraphFrame(v, e)

    def test_construction(self):
        g = self.g
        vertexIDs = map(lambda x: x[0], g.vertices.select("id").collect())
        assert sorted(vertexIDs) == [1, 2, 3]
        edgeActions = map(lambda x: x[0], g.edges.select("action").collect())
        assert sorted(edgeActions) == ["follow", "hate", "love"]

    def test_degrees(self):
        g = self.g
        outDeg = g.outDegrees()
        self.assertSetEqual(set(outDeg.columns), {"id", "outDegree"})
        inDeg = g.inDegrees()
        self.assertSetEqual(set(inDeg.columns), {"id", "inDegree"})
        deg = g.degrees()
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

    def test_connected_components(self):
        v = self.sqlContext.createDataFrame([
        (0L, "a", "b")], ["id", "vattr", "gender"])
        e = self.sqlContext.createDataFrame([(0L, 0L, 1L)], ["src", "dst", "test"]).filter("src > 10")
        g = GraphFrame(v, e)
        comps = g.connectedComponents()
        self._hasCols(comps, vcols=['id', 'component', 'vattr', 'gender'])
        self.assertEqual(comps.vertices.count(), 1)
        self.assertEqual(comps.edges.count(), 0)

    def test_connected_components2(self):
        v = self.sqlContext.createDataFrame([(0L, "a0", "b0"), (1L, "a1", "b1")], ["id", "A", "B"])
        e = self.sqlContext.createDataFrame([(0L, 1L, "a01", "b01")], ["src", "dst", "A", "B"])
        g = GraphFrame(v, e)
        comps = g.connectedComponents()
        self._hasCols(comps, vcols=['id', 'component', 'A', 'B'])
        self.assertEqual(comps.vertices.count(), 2)
        self.assertEqual(comps.edges.count(), 1)

    def test_label_progagation(self):
        n = 5
        g = self._graph("twoBlobs", n)
        labels = g.labelPropagation(maxSteps=4 * n)
        labels1 = labels.vertices.filter("id < 5").select("label").collect()
        all1 = set([x.label for x in labels1])
        self.assertEqual(all1, set([0]))
        labels2 = labels.vertices.filter("id >= 5").select("label").collect()
        all2 = set([x.label for x in labels2])
        self.assertEqual(all2, set([n]))

    def test_page_rank(self):
        n = 100
        g = self._graph("star", n)
        resetProb = 0.15
        errorTol = 1.0e-5
        pr = g.pageRank(resetProb, tolerance=errorTol)
        self._hasCols(pr, vcols=['id', 'weight'], ecols=['src', 'dst', 'weight'])

    def test_shortest_paths(self):
        edges = [(1, 2), (1, 5), (2, 3), (2, 5), (3, 4), (4, 5), (4, 6)]
        all_edges = [z for (a, b) in edges for z in [(a, b), (b, a)]]
        edges = self.sqlContext.createDataFrame(all_edges, ["src", "dst"])
        vertices = self.sqlContext.createDataFrame([(i,) for i in range(1, 7)], ["id"])
        g = GraphFrame(vertices, edges)
        landmarks = [1, 4]
        g2 = g.shortestPaths(landmarks)
        self._hasCols(g2, vcols=["id", "distance"])

    def test_svd_plus_plus(self):
        g = self._graph("ALSSyntheticData")
        (g2, cost) = g.svdPlusPlus()
        self._hasCols(g2, vcols=['id', 'column1', 'column2', 'column3'],
                      ecols=['src', 'dst', 'ecolumn1'])

    def test_strongly_connected_components(self):
        # Simple island test
        vertices = self.sqlContext.createDataFrame([(i,) for i in range(1, 6)], ["id"])
        edges = self.sqlContext.createDataFrame([(7, 8)], ["src", "dst"])
        g = GraphFrame(vertices, edges)
        c = g.stronglyConnectedComponents(5)
        for row in c.vertices.collect():
            self.assertEqual(row.id, row.component)

    def test_triangle_counts(self):
        edges = self.sqlContext.createDataFrame([(0, 1), (1, 2), (2, 0)], ["src", "dst"])
        vertices = self.sqlContext.createDataFrame([(0,), (1,), (2,)], ["id"])
        g = GraphFrame(vertices, edges)
        c = g.triangleCount()
        for row in c.vertices.select("id", "count").collect():
            self.assertEqual(row.count, 1)


