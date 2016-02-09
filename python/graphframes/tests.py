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

from .graphframe import GraphFrame


class GraphFrameTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.sc = SparkContext('local[4]', cls.__name__)
        cls.sql = SQLContext(cls.sc)

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
