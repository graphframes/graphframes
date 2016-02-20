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

def _from_java_gf(jgf, sqlContext):
    """
    (internal) creates a python GraphFrame wrapper from a java GraphFrame.

    :param jgf:
    :return:
    """
    pv = DataFrame(jgf.vertices(), sqlContext)
    pe = DataFrame(jgf.edges(), sqlContext)
    return GraphFrame(pv, pe)

def _java_api(jsc):
    javaClassName = "org.graphframes.GraphFramePythonAPI"
    return jsc._jvm.Thread.currentThread().getContextClassLoader().loadClass(javaClassName) \
            .newInstance()


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
        self._jvm_gf_api = _java_api(self._sc)
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

    def __repr__(self):
        return self._jvm_graph.toString()

    def outDegrees(self):
        """
        The out-degree of each vertex in the graph, returned as a DataFrame with two columns:
         - [[GraphFrame.ID]] the ID of the vertex
         - "outDegree" (integer) storing the out-degree of the vertex

        Note that vertices with 0 out-edges are not returned in the result.
        """
        jdf = self._jvm_graph.outDegrees()
        return DataFrame(jdf, self._sqlContext)

    def inDegrees(self):
        """
        The in-degree of each vertex in the graph, returned as a DataFame with two columns:
         - [[GraphFrame.ID]] the ID of the vertex
         - "inDegree" (int) storing the in-degree of the vertex

        Note that vertices with 0 in-edges are not returned in the result.
        """
        jdf = self._jvm_graph.inDegrees()
        return DataFrame(jdf, self._sqlContext)

    def degrees(self):
        """
        The degree of each vertex in the graph, returned as a DataFrame with two columns:
         - [[GraphFrame.ID]] the ID of the vertex
         - 'degree' (integer) the degree of the vertex

        Note that vertices with 0 edges are not returned in the result.
        """
        jdf = self._jvm_graph.degrees()
        return DataFrame(jdf, self._sqlContext)

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
        builder = self._jvm_graph.bfs(fromExpr, toExpr).maxPathLength(maxPathLength)
        if edgeFilter is not None:
            builder.edgeFilter(edgeFilter)
        jdf = builder.run()
        return DataFrame(jdf, self._sqlContext)

    # Standard algorithms

    def connectedComponents(self):
        """
        Computes the connected components of the graph.

        :return:
        """
        jgf = self._jvm_graph.connectedComponents().run()
        return _from_java_gf(jgf, self._sqlContext)

    def labelPropagation(self, maxSteps):
        """
        Runs static label propagation for detecting communities in networks.

        :param maxSteps: the number of super steps to be performed.
        :return:
        """
        jgf = self._jvm_graph.labelPropagation().maxSteps(maxSteps).run()
        return _from_java_gf(jgf, self._sqlContext)

    def pageRank(self, resetProbability = 0.15, sourceId = None, numIter = None,
                 tol = None):
        """
        Runs the PageRank algorithm on the graph.
        Note: Exactly one of fixed_num_iter or tolerance must be set.

        :param resetProbability:
        :param sourceId: (optional) the source vertex for a personalized PageRank.
        :param numIter: If set, the algorithm is run for a fixed number
               of iterations. This may not be set if the `tol` parameter is set.
        :param tol: If set, the algorithm is run until the given tolerance.
               This may not be set if the `numIter` parameter is set.
        :return:
        """
        builder = self._jvm_graph.pageRank().resetProbability(resetProbability)
        if sourceId is not None:
            builder = builder.sourceId(sourceId)
        if numIter is not None:
            builder = builder.numIter(numIter)
            assert tol is None, "Exactly one of numIter or tol shoud be set."
        else:
            assert tol is not None, "Exactly one of numIter or tol shoud be set."
            builder = builder.tol(tol)
        jgf = builder.run()
        return _from_java_gf(jgf, self._sqlContext)

    def shortestPaths(self, landmarks):
        """
        Runs the shortest path algorithm from a set of landmark vertices in the graph.

        :param landmarks: a set of landmarks
        :return:
        """
        jgf = self._jvm_graph.shortestPaths().landmarks(landmarks).run()
        return _from_java_gf(jgf, self._sqlContext)

    def stronglyConnectedComponents(self, numIter):
        """
        Runs the strongly connected components algorithm on this graph.

        :param numIter: the number of iterations to run.
        :return:
        """
        jgf = self._jvm_graph.stronglyConnectedComponents().numIter(numIter).run()
        return _from_java_gf(jgf, self._sqlContext)

    def svdPlusPlus(self, rank = 10, maxIter = 2, minValue = 0.0, maxValue = 5.0,
                    gamma1 = 0.007, gamma2 = 0.007, gamma6 = 0.005, gamma7 = 0.015):
        """
        Runs the SVD++ algorithm.

        :return:
        """
        # This call is actually useless, because one needs to build the configuration first...
        builder = self._jvm_graph.svdPlusPlus()
        builder.rank(rank).maxIter(maxIter).minValue(minValue).maxValue(maxValue)
        builder.gamma1(gamma1).gamma2(gamma2).gamma6(gamma6).gamma7(gamma7)
        jgf = builder.run()
        loss = builder.loss()
        gf = _from_java_gf(jgf, self._sqlContext)
        return (gf, loss)

    def triangleCount(self):
        """
        Counts the number of triangles passing through each vertex in this graph.

        :return:
        """
        jgf = self._jvm_graph.triangleCount().run()
        return _from_java_gf(jgf, self._sqlContext)


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
