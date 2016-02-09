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

from pyspark import SparkContext
from pyspark.sql import DataFrame, SQLContext


class GraphFrame(object):
    """
    Represents a graph with vertices and edges stored as DataFrames.

    :param vertices:  :class:`DataFrame` holding vertex information.
                      Must contain a column named "id" that stores unique
                      vertex IDs.
    :param edges:  :class:`DataFrame` holding edge information.
                   Must contain two columns "src" and "dst" storing source
                   vertex IDs and destination vertex IDs of edges, respectively.

    >>> localVertices = [(1,"A"), (2,"B"), (3, "C")]
    >>> localEdges = [(1,2,"love"), (2,1,"hate"), (2,3,"follow")]
    >>> v = sqlContext.createDataFrame(localVertices, ["id", "name"])
    >>> e = sqlContext.createDataFrame(localEdges, ["src", "dst", "action"])
    >>> g = GraphFrame(v, e)
    """

    def __init__(self, v, e):
        self._vertices = v
        self._edges = e
        self._sqlContext = v.sql_ctx
        self._sc = self._sqlContext._sc

        self._sc._jvm.org.apache.spark.ml.feature.Tokenizer()

        javaClassName = "org.graphframes.GraphFramePythonAPI"
        self._jvm_gf_api = \
            self._sc._jvm.Thread.currentThread().getContextClassLoader().loadClass(javaClassName)\
                .newInstance()

        self._jvm_graph = self._jvm_gf_api.createGraph(v._jdf, e._jdf)

        self.ID = self._jvm_gf_api.ID()
        self.SRC = self._jvm_gf_api.SRC()
        self.DST = self._jvm_gf_api.DST()
        self._ATTR = self._jvm_gf_api.ATTR()

        assert self.ID in v.columns,\
            "Vertex ID column '%s' missing from vertex DataFrame, which has columns: %s" %\
            (self.ID, ",".join(v.columns))
        assert self.SRC in e.columns,\
            "Source vertex ID column '%s' missing from edge DataFrame, which has columns: %s" %\
            (self.SRC, ",".join(e.columns))
        assert self.DST in e.columns,\
            "Destination vertex ID column '%s' missing from edge DataFrame, which has columns: %s"%\
            (self.DST, ",".join(e.columns))

    @property
    def vertices(self):
        """
        :class:`DataFrame` holding vertex information, with unique column "id"
        for vertex IDs.
        """
        return self._vertices

    @property
    def edges(self):
        """
        :class:`DataFrame` holding edge information, with unique columns "src" and
        "dst" storing source vertex IDs and destination vertex IDs of edges,
        respectively.
        """
        return self._edges

    def find(self, pattern):
        """
        Motif finding.
        TODO: Copy doc from Scala
        """
        jdf = self._jvm_graph.find(pattern)
        return DataFrame(jdf, self._sqlContext)

    def bfs(self, fromExpr, toExpr, edgeFilter=None, maxPathLength=10):
        """
        Breadth-first search (BFS)
        """
        builder = self._jvm_graph.bfs(fromExpr, toExpr).setMaxPathLength(maxPathLength)
        if edgeFilter is not None:
            builder.setEdgeFilter(edgeFilter)
        jdf = builder.run()
        return DataFrame(jdf, self._sqlContext)


def _test():
    import doctest
    import graphframe
    globs = graphframe.__dict__.copy()
    globs['sc'] = SparkContext('local[4]', 'PythonTest', batchSize=2)
    globs['sqlContext'] = SQLContext(globs['sc'])
    (failure_count, test_count) = doctest.testmod(
        globs=globs, optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
