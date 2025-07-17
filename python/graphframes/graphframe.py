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

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from pyspark.storagelevel import StorageLevel
from pyspark.version import __version__

if __version__[:3] >= "3.4":
    from pyspark.sql.utils import is_remote
else:
    # All the Connect-related utilities are accessible starting from 3.4.x
    def is_remote() -> bool:
        return False


from pyspark.sql import SparkSession

from graphframes.classic.graphframe import GraphFrame as GraphFrameClassic
from graphframes.lib import Pregel

if __version__[:3] >= "3.4":
    from graphframes.connect.graphframe_client import GraphFrameConnect
else:

    class GraphFrameConnect:
        def __init__(self, *args, **kwargs) -> None:
            raise ValueError("Unreachable error happened!")


if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


class GraphFrame:
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
    >>> v = spark.createDataFrame(localVertices, ["id", "name"])
    >>> e = spark.createDataFrame(localEdges, ["src", "dst", "action"])
    >>> g = GraphFrame(v, e)
    """

    @staticmethod
    def _from_impl(impl: GraphFrameClassic | GraphFrameConnect) -> "GraphFrame":
        return GraphFrame(impl.vertices, impl.edges)

    def __init__(self, v: DataFrame, e: DataFrame) -> None:
        if is_remote():
            self._impl = GraphFrameConnect(v, e)
        else:
            self._impl = GraphFrameClassic(v, e)

    @property
    def vertices(self) -> DataFrame:
        """
        :class:`DataFrame` holding vertex information, with unique column "id"
        for vertex IDs.
        """
        return self._impl.vertices

    @property
    def edges(self) -> DataFrame:
        """
        :class:`DataFrame` holding edge information, with unique columns "src" and
        "dst" storing source vertex IDs and destination vertex IDs of edges,
        respectively.
        """
        return self._impl.edges

    def __repr__(self) -> str:
        return self._impl.__repr__()

    def cache(self) -> "GraphFrame":
        """Persist the dataframe representation of vertices and edges of the graph with the default
        storage level.
        """
        return GraphFrame._from_impl(self._impl.cache())

    def persist(self, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY) -> "GraphFrame":
        """Persist the dataframe representation of vertices and edges of the graph with the given
        storage level.
        """
        return GraphFrame._from_impl(self._impl.persist(storageLevel=storageLevel))

    def unpersist(self, blocking: bool = False) -> "GraphFrame":
        """Mark the dataframe representation of vertices and edges of the graph as non-persistent,
        and remove all blocks for it from memory and disk.
        """
        return GraphFrame._from_impl(self._impl.unpersist(blocking=blocking))

    @property
    def outDegrees(self) -> DataFrame:
        """
        The out-degree of each vertex in the graph, returned as a DataFrame with two columns:
         - "id": the ID of the vertex
         - "outDegree" (integer) storing the out-degree of the vertex

        Note that vertices with 0 out-edges are not returned in the result.

        :return:  DataFrame with new vertices column "outDegree"
        """
        return self._impl.outDegrees

    @property
    def inDegrees(self) -> DataFrame:
        """
        The in-degree of each vertex in the graph, returned as a DataFame with two columns:
         - "id": the ID of the vertex
         - "inDegree" (int) storing the in-degree of the vertex

        Note that vertices with 0 in-edges are not returned in the result.

        :return:  DataFrame with new vertices column "inDegree"
        """
        return self._impl.inDegrees

    @property
    def degrees(self) -> DataFrame:
        """
        The degree of each vertex in the graph, returned as a DataFrame with two columns:
         - "id": the ID of the vertex
         - 'degree' (integer) the degree of the vertex

        Note that vertices with 0 edges are not returned in the result.

        :return:  DataFrame with new vertices column "degree"
        """
        return self._impl.degrees

    @property
    def triplets(self) -> DataFrame:
        """
        The triplets (source vertex)-[edge]->(destination vertex) for all edges in the graph.

        Returned as a :class:`DataFrame` with three columns:
         - "src": source vertex with schema matching 'vertices'
         - "edge": edge with schema matching 'edges'
         - 'dst': destination vertex with schema matching 'vertices'

        :return:  DataFrame with columns 'src', 'edge', and 'dst'
        """
        return self._impl.triplets

    @property
    def pregel(self) -> Pregel:
        """
        Get the :class:`graphframes.lib.Pregel` object for running pregel.

        See :class:`graphframes.lib.Pregel` for more details.
        """
        return self._impl.pregel

    def find(self, pattern: str) -> DataFrame:
        """
        Motif finding.

        See Scala documentation for more details.

        :param pattern:  String describing the motif to search for.
        :return:  DataFrame with one Row for each instance of the motif found
        """
        return self._impl.find(pattern=pattern)

    def filterVertices(self, condition: str | Column) -> "GraphFrame":
        """
        Filters the vertices based on expression, remove edges containing any dropped vertices.

        :param condition: String or Column describing the condition expression for filtering.
        :return: GraphFrame with filtered vertices and edges.
        """
        return GraphFrame._from_impl(self._impl.filterVertices(condition=condition))

    def filterEdges(self, condition: str | Column) -> "GraphFrame":
        """
        Filters the edges based on expression, keep all vertices.

        :param condition: String or Column describing the condition expression for filtering.
        :return: GraphFrame with filtered edges.
        """

        return GraphFrame._from_impl(self._impl.filterEdges(condition=condition))

    def dropIsolatedVertices(self) -> "GraphFrame":
        """
        Drops isolated vertices, vertices are not contained in any edges.

        :return: GraphFrame with filtered vertices.
        """
        return GraphFrame._from_impl(self._impl.dropIsolatedVertices())

    def bfs(
        self,
        fromExpr: str,
        toExpr: str,
        edgeFilter: str | None = None,
        maxPathLength: int = 10,
    ) -> DataFrame:
        """
        Breadth-first search (BFS).

        See Scala documentation for more details.

        :return: DataFrame with one Row for each shortest path between matching vertices.
        """
        return self._impl.bfs(
            fromExpr=fromExpr,
            toExpr=toExpr,
            edgeFilter=edgeFilter,
            maxPathLength=maxPathLength,
        )

    def aggregateMessages(
        self,
        aggCol: Column | str,
        sendToSrc: Column | str | None = None,
        sendToDst: Column | str | None = None,
    ) -> DataFrame:
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
        return self._impl.aggregateMessages(aggCol=aggCol, sendToSrc=sendToSrc, sendToDst=sendToDst)

    # Standard algorithms

    def connectedComponents(
        self,
        algorithm: str = "graphframes",
        checkpointInterval: int = 2,
        broadcastThreshold: int = 1000000,
        useLabelsAsComponents: bool = False,
    ) -> DataFrame:
        """
        Computes the connected components of the graph.

        See Scala documentation for more details.

        :param algorithm: connected components algorithm to use (default: "graphframes")
          Supported algorithms are "graphframes" and "graphx".
        :param checkpointInterval: checkpoint interval in terms of number of iterations (default: 2)
        :param broadcastThreshold: broadcast threshold in propagating component assignments
          (default: 1000000)
        :param useLabelsAsComponents: if True, uses the vertex labels as components, otherwise will
          use longs

        :return: DataFrame with new vertices column "component"
        """
        return self._impl.connectedComponents(
            algorithm=algorithm,
            checkpointInterval=checkpointInterval,
            broadcastThreshold=broadcastThreshold,
            useLabelsAsComponents=useLabelsAsComponents,
        )

    def labelPropagation(self, maxIter: int) -> DataFrame:
        """
        Runs static label propagation for detecting communities in networks.

        See Scala documentation for more details.

        :param maxIter: the number of iterations to be performed
        :return: DataFrame with new vertices column "label"
        """
        return self._impl.labelPropagation(maxIter=maxIter)

    def pageRank(
        self,
        resetProbability: float = 0.15,
        sourceId: Optional[Any] = None,
        maxIter: Optional[int] = None,
        tol: Optional[float] = None,
    ) -> "GraphFrame":
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
        return GraphFrame._from_impl(
            self._impl.pageRank(
                resetProbability=resetProbability,
                sourceId=sourceId,
                maxIter=maxIter,
                tol=tol,
            )
        )

    def parallelPersonalizedPageRank(
        self,
        resetProbability: float = 0.15,
        sourceIds: Optional[list[Any]] = None,
        maxIter: Optional[int] = None,
    ) -> "GraphFrame":
        """
        Run the personalized PageRank algorithm on the graph,
        from the provided list of sources in parallel for a fixed number of iterations.

        See Scala documentation for more details.

        :param resetProbability: Probability of resetting to a random vertex
        :param sourceIds: the source vertices for a personalized PageRank
        :param maxIter: the fixed number of iterations this algorithm runs
        :return:  GraphFrame with new vertices column "pageranks" and new edges column "weight"
        """
        return GraphFrame._from_impl(
            self._impl.parallelPersonalizedPageRank(
                resetProbability=resetProbability, sourceIds=sourceIds, maxIter=maxIter
            )
        )

    def shortestPaths(self, landmarks: list[Any]) -> DataFrame:
        """
        Runs the shortest path algorithm from a set of landmark vertices in the graph.

        See Scala documentation for more details.

        :param landmarks: a set of one or more landmarks
        :return: DataFrame with new vertices column "distances"
        """
        return self._impl.shortestPaths(landmarks=landmarks)

    def stronglyConnectedComponents(self, maxIter: int) -> DataFrame:
        """
        Runs the strongly connected components algorithm on this graph.

        See Scala documentation for more details.

        :param maxIter: the number of iterations to run
        :return: DataFrame with new vertex column "component"
        """
        return self._impl.stronglyConnectedComponents(maxIter=maxIter)

    def svdPlusPlus(
        self,
        rank: int = 10,
        maxIter: int = 2,
        minValue: float = 0.0,
        maxValue: float = 5.0,
        gamma1: float = 0.007,
        gamma2: float = 0.007,
        gamma6: float = 0.005,
        gamma7: float = 0.015,
    ) -> tuple[DataFrame, float]:
        """
        Runs the SVD++ algorithm.

        See Scala documentation for more details.

        :return: Tuple of DataFrame with new vertex columns storing learned model, and loss value
        """
        return self._impl.svdPlusPlus(
            rank=rank,
            maxIter=maxIter,
            minValue=minValue,
            maxValue=maxValue,
            gamma1=gamma1,
            gamma2=gamma2,
            gamma6=gamma6,
            gamma7=gamma7,
        )

    def triangleCount(self) -> DataFrame:
        """
        Counts the number of triangles passing through each vertex in this graph.

        See Scala documentation for more details.

        :return:  DataFrame with new vertex column "count"
        """
        return self._impl.triangleCount()

    def powerIterationClustering(
        self, k: int, maxIter: int, weightCol: Optional[str] = None
    ) -> DataFrame:
        """
        Power Iteration Clustering (PIC), a scalable graph clustering algorithm developed by Lin and Cohen.
        From the abstract: PIC finds a very low-dimensional embedding of a dataset using truncated power iteration
        on a normalized pair-wise similarity matrix of the data.

        :param k: the numbers of clusters to create
        :param maxIter: param for maximum number of iterations (>= 0)
        :param weightCol: optional name of weight column, 1.0 is used if not provided

        :return: DataFrame with new column "cluster"
        """  # noqa: E501
        return self._impl.powerIterationClustering(k, maxIter, weightCol)


def _test():
    import doctest

    import graphframe

    globs = graphframe.__dict__.copy()
    globs["spark"] = SparkSession.builder.master("local[4]").appName("PythonTest").getOrCreate()
    (failure_count, test_count) = doctest.testmod(
        globs=globs, optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE
    )
    globs["spark"].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
