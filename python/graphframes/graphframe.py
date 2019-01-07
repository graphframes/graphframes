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
if sys.version > '3':
    basestring = str

from pyspark import SparkContext
from pyspark.sql import Column, DataFrame, SQLContext
from pyspark.storagelevel import StorageLevel

from graphframes.lib import Pregel


def _from_java_gf(jgf, sqlContext):
    """
    (internal) creates a python GraphFrame wrapper from a java GraphFrame.

    :param jgf:
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

    :param v:  :class:`DataFrame` holding vertex information.
               Must contain a column named "id" that stores unique
               vertex IDs.
    :param e:  :class:`DataFrame` holding edge information.
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
        self._jvm_gf_api = _java_api(self._sc)

        self.ID = self._jvm_gf_api.ID()
        self.SRC = self._jvm_gf_api.SRC()
        self.DST = self._jvm_gf_api.DST()
        self._ATTR = self._jvm_gf_api.ATTR()

        # Check that provided DataFrames contain required columns
        if self.ID not in v.columns:
            raise ValueError(
                "Vertex ID column {} missing from vertex DataFrame, which has columns: {}"
                .format(self.ID, ",".join(v.columns)))
        if self.SRC not in e.columns:
            raise ValueError(
                "Source vertex ID column {} missing from edge DataFrame, which has columns: {}"
                .format(self.SRC, ",".join(e.columns)))
        if self.DST not in e.columns:
            raise ValueError(
                "Destination vertex ID column {} missing from edge DataFrame, which has columns: {}"
                .format(self.DST, ",".join(e.columns)))

        self._jvm_graph = self._jvm_gf_api.createGraph(v._jdf, e._jdf)

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

    def cache(self):
        """ Persist the dataframe representation of vertices and edges of the graph with the default
        storage level.
        """
        self._jvm_graph.cache()
        return self

    def persist(self, storageLevel=StorageLevel.MEMORY_ONLY):
        """Persist the dataframe representation of vertices and edges of the graph with the given
        storage level.
        """
        javaStorageLevel = self._sc._getJavaStorageLevel(storageLevel)
        self._jvm_graph.persist(javaStorageLevel)
        return self

    def unpersist(self, blocking=False):
        """Mark the dataframe representation of vertices and edges of the graph as non-persistent,
        and remove all blocks for it from memory and disk.
        """
        self._jvm_graph.unpersist(blocking)
        return self

    @property
    def outDegrees(self):
        """
        The out-degree of each vertex in the graph, returned as a DataFrame with two columns:
         - "id": the ID of the vertex
         - "outDegree" (integer) storing the out-degree of the vertex

        Note that vertices with 0 out-edges are not returned in the result.

        :return:  DataFrame with new vertices column "outDegree"
        """
        jdf = self._jvm_graph.outDegrees()
        return DataFrame(jdf, self._sqlContext)

    @property
    def inDegrees(self):
        """
        The in-degree of each vertex in the graph, returned as a DataFame with two columns:
         - "id": the ID of the vertex
         - "inDegree" (int) storing the in-degree of the vertex

        Note that vertices with 0 in-edges are not returned in the result.

        :return:  DataFrame with new vertices column "inDegree"
        """
        jdf = self._jvm_graph.inDegrees()
        return DataFrame(jdf, self._sqlContext)

    @property
    def degrees(self):
        """
        The degree of each vertex in the graph, returned as a DataFrame with two columns:
         - "id": the ID of the vertex
         - 'degree' (integer) the degree of the vertex

        Note that vertices with 0 edges are not returned in the result.

        :return:  DataFrame with new vertices column "degree"
        """
        jdf = self._jvm_graph.degrees()
        return DataFrame(jdf, self._sqlContext)

    @property
    def triplets(self):
        """
        The triplets (source vertex)-[edge]->(destination vertex) for all edges in the graph.

        Returned as a :class:`DataFrame` with three columns:
         - "src": source vertex with schema matching 'vertices'
         - "edge": edge with schema matching 'edges'
         - 'dst': destination vertex with schema matching 'vertices'

        :return:  DataFrame with columns 'src', 'edge', and 'dst'
        """
        jdf = self._jvm_graph.triplets()
        return DataFrame(jdf, self._sqlContext)

    @property
    def pregel(self):
        """
        Get the :class:`graphframes.lib.Pregel` object for running pregel.

        See :class:`graphframes.lib.Pregel` for more details.
        """
        return Pregel(self)

    def find(self, pattern):
        """
        Motif finding.

        See Scala documentation for more details.

        :param pattern:  String describing the motif to search for.
        :return:  DataFrame with one Row for each instance of the motif found
        """
        jdf = self._jvm_graph.find(pattern)
        return DataFrame(jdf, self._sqlContext)

    def filterVertices(self, condition):
        """
        Filters the vertices based on expression, remove edges containing any dropped vertices.
        
        :param condition: String or Column describing the condition expression for filtering.
        :return: GraphFrame with filtered vertices and edges. 
        """

        if isinstance(condition, basestring):
            jdf = self._jvm_graph.filterVertices(condition)
        elif isinstance(condition, Column):
            jdf = self._jvm_graph.filterVertices(condition._jc)
        else:
            raise TypeError("condition should be string or Column")
        return _from_java_gf(jdf, self._sqlContext)

    def filterEdges(self, condition):
        """
        Filters the edges based on expression, keep all vertices.
        
        :param condition: String or Column describing the condition expression for filtering.
        :return: GraphFrame with filtered edges. 
        """
        if isinstance(condition, basestring):
            jdf = self._jvm_graph.filterEdges(condition)
        elif isinstance(condition, Column):
            jdf = self._jvm_graph.filterEdges(condition._jc)
        else:
            raise TypeError("condition should be string or Column")
        return _from_java_gf(jdf, self._sqlContext)

    def dropIsolatedVertices(self):
        """
        Drops isolated vertices, vertices are not contained in any edges.

        :return: GraphFrame with filtered vertices. 
        """
        jdf = self._jvm_graph.dropIsolatedVertices()
        return _from_java_gf(jdf, self._sqlContext)

    def bfs(self, fromExpr, toExpr, edgeFilter=None, maxPathLength=10):
        """
        Breadth-first search (BFS).

        See Scala documentation for more details.

        :return: DataFrame with one Row for each shortest path between matching vertices.
        """
        builder = self._jvm_graph.bfs()\
            .fromExpr(fromExpr)\
            .toExpr(toExpr)\
            .maxPathLength(maxPathLength)
        if edgeFilter is not None:
            builder.edgeFilter(edgeFilter)
        jdf = builder.run()
        return DataFrame(jdf, self._sqlContext)

    def aggregateMessages(self, aggCol, sendToSrc=None, sendToDst=None):
        """
        Aggregates messages from the neighbours.

        When specifying the messages and aggregation function, the user may reference columns using
        the static methods in :class:`graphframes.lib.AggregateMessages`.

        See Scala documentation for more details.

        :param aggCol: the requested aggregation output either as
            :class:`pyspark.sql.Column` or SQL expression string
        :param sendToSrc: message sent to the source vertex of each triplet either as
            :class:`pyspark.sql.Column` or SQL expression string (default: None)
        :param sendToDst: message sent to the destination vertex of each triplet either as
            :class:`pyspark.sql.Column` or SQL expression string (default: None)

        :return: DataFrame with columns for the vertex ID and the resulting aggregated message
        """
        # Check that either sendToSrc, sendToDst, or both are provided
        if sendToSrc is None and sendToDst is None:
            raise ValueError("Either `sendToSrc`, `sendToDst`, or both have to be provided")
        builder = self._jvm_graph.aggregateMessages()
        if sendToSrc is not None:
            if isinstance(sendToSrc, Column):
                builder.sendToSrc(sendToSrc._jc)
            elif isinstance(sendToSrc, basestring):
                builder.sendToSrc(sendToSrc)
            else:
                raise TypeError("Provide message either as `Column` or `str`")
        if sendToDst is not None:
            if isinstance(sendToDst, Column):
                builder.sendToDst(sendToDst._jc)
            elif isinstance(sendToDst, basestring):
                builder.sendToDst(sendToDst)
            else:
                raise TypeError("Provide message either as `Column` or `str`")
        if isinstance(aggCol, Column):
            jdf = builder.agg(aggCol._jc)
        else:
            jdf = builder.agg(aggCol)
        return DataFrame(jdf, self._sqlContext)

    # Standard algorithms

    def connectedComponents(self, algorithm = "graphframes", checkpointInterval = 2,
                            broadcastThreshold = 1000000):
        """
        Computes the connected components of the graph.

        See Scala documentation for more details.

        :param algorithm: connected components algorithm to use (default: "graphframes")
          Supported algorithms are "graphframes" and "graphx".
        :param checkpointInterval: checkpoint interval in terms of number of iterations (default: 2)
        :param broadcastThreshold: broadcast threshold in propagating component assignments
          (default: 1000000)

        :return: DataFrame with new vertices column "component"
        """
        jdf = self._jvm_graph.connectedComponents() \
            .setAlgorithm(algorithm) \
            .setCheckpointInterval(checkpointInterval) \
            .setBroadcastThreshold(broadcastThreshold) \
            .run()
        return DataFrame(jdf, self._sqlContext)

    def labelPropagation(self, maxIter):
        """
        Runs static label propagation for detecting communities in networks.

        See Scala documentation for more details.

        :param maxIter: the number of iterations to be performed
        :return: DataFrame with new vertices column "label"
        """
        jdf = self._jvm_graph.labelPropagation().maxIter(maxIter).run()
        return DataFrame(jdf, self._sqlContext)

    def pageRank(self, resetProbability = 0.15, sourceId = None, maxIter = None,
                 tol = None):
        """
        Runs the PageRank algorithm on the graph.
        Note: Exactly one of fixed_num_iter or tolerance must be set.

        See Scala documentation for more details.

        :param resetProbability: Probability of resetting to a random vertex.
        :param sourceId: (optional) the source vertex for a personalized PageRank.
        :param maxIter: If set, the algorithm is run for a fixed number
               of iterations. This may not be set if the `tol` parameter is set.
        :param tol: If set, the algorithm is run until the given tolerance.
               This may not be set if the `numIter` parameter is set.
        :return:  GraphFrame with new vertices column "pagerank" and new edges column "weight"
        """
        builder = self._jvm_graph.pageRank().resetProbability(resetProbability)
        if sourceId is not None:
            builder = builder.sourceId(sourceId)
        if maxIter is not None:
            builder = builder.maxIter(maxIter)
            assert tol is None, "Exactly one of maxIter or tol should be set."
        else:
            assert tol is not None, "Exactly one of maxIter or tol should be set."
            builder = builder.tol(tol)
        jgf = builder.run()
        return _from_java_gf(jgf, self._sqlContext)

    def parallelPersonalizedPageRank(self, resetProbability = 0.15, sourceIds = None,
                                     maxIter = None):
        """
        Run the personalized PageRank algorithm on the graph,
        from the provided list of sources in parallel for a fixed number of iterations.

        See Scala documentation for more details.

        :param resetProbability: Probability of resetting to a random vertex
        :param sourceIds: the source vertices for a personalized PageRank
        :param maxIter: the fixed number of iterations this algorithm runs
        :return:  GraphFrame with new vertices column "pageranks" and new edges column "weight"
        """
        assert sourceIds is not None and len(sourceIds) > 0, "Source vertices Ids sourceIds must be provided"
        assert maxIter is not None, "Max number of iterations maxIter must be provided"
        sourceIds = self._sc._jvm.PythonUtils.toArray(sourceIds)
        builder = self._jvm_graph.parallelPersonalizedPageRank()
        builder = builder.resetProbability(resetProbability)
        builder = builder.sourceIds(sourceIds)
        builder = builder.maxIter(maxIter)
        jgf = builder.run()
        return _from_java_gf(jgf, self._sqlContext)

    def shortestPaths(self, landmarks):
        """
        Runs the shortest path algorithm from a set of landmark vertices in the graph.

        See Scala documentation for more details.

        :param landmarks: a set of one or more landmarks
        :return: DataFrame with new vertices column "distances"
        """
        jdf = self._jvm_graph.shortestPaths().landmarks(landmarks).run()
        return DataFrame(jdf, self._sqlContext)

    def stronglyConnectedComponents(self, maxIter):
        """
        Runs the strongly connected components algorithm on this graph.

        See Scala documentation for more details.

        :param maxIter: the number of iterations to run
        :return: DataFrame with new vertex column "component"
        """
        jdf = self._jvm_graph.stronglyConnectedComponents().maxIter(maxIter).run()
        return DataFrame(jdf, self._sqlContext)

    def svdPlusPlus(self, rank = 10, maxIter = 2, minValue = 0.0, maxValue = 5.0,
                    gamma1 = 0.007, gamma2 = 0.007, gamma6 = 0.005, gamma7 = 0.015):
        """
        Runs the SVD++ algorithm.

        See Scala documentation for more details.

        :return: Tuple of DataFrame with new vertex columns storing learned model, and loss value
        """
        # This call is actually useless, because one needs to build the configuration first...
        builder = self._jvm_graph.svdPlusPlus()
        builder.rank(rank).maxIter(maxIter).minValue(minValue).maxValue(maxValue)
        builder.gamma1(gamma1).gamma2(gamma2).gamma6(gamma6).gamma7(gamma7)
        jdf = builder.run()
        loss = builder.loss()
        v = DataFrame(jdf, self._sqlContext)
        return (v, loss)

    def triangleCount(self):
        """
        Counts the number of triangles passing through each vertex in this graph.

        See Scala documentation for more details.

        :return:  DataFrame with new vertex column "count"
        """
        jdf = self._jvm_graph.triangleCount().run()
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
