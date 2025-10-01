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

import warnings
from typing import TYPE_CHECKING, Any

from pyspark.storagelevel import StorageLevel
from pyspark.sql import functions as F
from pyspark.version import __version__
from typing_extensions import override

if __version__[:3] >= "3.4":
    from pyspark.sql.utils import is_remote
else:
    # All the Connect-related utilities are accessible starting from 3.4.x
    def is_remote() -> bool:
        return False


from graphframes.classic.graphframe import GraphFrame as GraphFrameClassic
from graphframes.lib import Pregel

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from graphframes.connect.graphframes_client import GraphFrameConnect

"""Constant for the vertices ID column name."""
ID = "id"

"""Constant for the edge src column name."""
SRC = "src"

"""Constant for the edge dst column name."""
DST = "dst"

"""Constant for the edge column name."""
EDGE = "edge"


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
    def _from_impl(impl: "GraphFrameClassic | GraphFrameConnect") -> "GraphFrame":
        return GraphFrame(impl.vertices, impl.edges)

    def __init__(self, v: DataFrame, e: DataFrame) -> None:
        """
        Initialize a GraphFrame from vertex DataFrame and edges DataFrame.

        :param v: :class:`DataFrame` holding vertex information.
                Must contain a column named "id" that stores unique
                vertex IDs.
        :param e: :class:`DataFrame` holding edge information.
                Must contain two columns "src" and "dst" storing source
                vertex IDs and destination vertex IDs of edges, respectively.
        """
        self._impl: "GraphFrameClassic | GraphFrameConnect"
        if is_remote():
            from graphframes.connect.graphframes_client import GraphFrameConnect

            self._impl = GraphFrameConnect(v, e)  # ty: ignore[invalid-argument-type]
        else:
            self._impl = GraphFrameClassic(v, e)  # ty: ignore[invalid-argument-type]

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

    @property
    def nodes(self) -> DataFrame:
        """Alias to vertices."""
        return self.vertices

    @override
    def __repr__(self) -> str:
        return self._impl.__repr__()

    def cache(self) -> "GraphFrame":
        """Persist the dataframe representation of vertices and edges of the graph with the default
        storage level.
        """
        return GraphFrame._from_impl(self._impl.cache())

    def persist(
        self, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
    ) -> "GraphFrame":
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
        Get the :class:`graphframes.classic.pregel.Pregel`
        or :class`graphframes.connect.graphframes_client.Pregel`
        object for running pregel.

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

    def detectingCycles(
        self,
        checkpoint_interval: int = 2,
        use_local_checkpoints: bool = False,
        storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK_DESER,
    ) -> DataFrame:
        """Find all cycles in the graph.

        An implementation of the Rocha–Thatte cycle detection algorithm.
        Rocha, Rodrigo Caetano, and Bhalchandra D. Thatte. "Distributed cycle detection in
        large-scale sparse graphs." Proceedings of Simpósio Brasileiro de Pesquisa Operacional
        (SBPO’15) (2015): 1-11.

        Returns a DataFrame with ID and cycles, ID are not unique if there are multiple cycles
        starting from this ID. For the case of cycle 1 -> 2 -> 3 -> 1 all the vertices will have the
        same cycle! E.g.: 1 -> [1, 2, 3, 1] 2 -> [2, 3, 1, 2] 3 -> [3, 1, 2, 3]

        Deduplication of cycles should be done by the user!

        :param checkpoint_interval: Pregel checkpoint interval, default is 2
        :param use_local_checkpoints: should local checkpoints be used instead of checkpointDir
        :storage_level: the level of storage for both intermediate results and an output DataFrame

        :return: Persisted DataFrame with all the cycles
        """
        return self._impl.detectingCycles(
            checkpoint_interval, use_local_checkpoints, storage_level
        )

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
        aggCol: list[Column | str] | Column,
        sendToSrc: list[Column | str] | Column | str | None = None,
        sendToDst: list[Column | str] | Column | str | None = None,
        intermediate_storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK_DESER,
    ) -> DataFrame:
        """
        Aggregates messages from the neighbours.

        When specifying the messages and aggregation function, the user may reference columns using
        the static methods in :class:`graphframes.lib.AggregateMessages`.

        See Scala documentation for more details.

        Warning! The result of this method is persisted DataFrame object! Users should handle unpersist
        to avoid possible memory leaks!

        :param aggCol: the requested aggregation output either as a collection of
            :class:`pyspark.sql.Column` or SQL expression string
        :param sendToSrc: message sent to the source vertex of each triplet either as
            a collection of :class:`pyspark.sql.Column` or SQL expression string (default: None)
        :param sendToDst: message sent to the destination vertex of each triplet either as
            collection of :class:`pyspark.sql.Column` or SQL expression string (default: None)
        :param intermediate_storage_level: the level of intermediate storage that will be used
            for both intermediate result and the output.

        :return: Persisted DataFrame with columns for the vertex ID and the resulting aggregated message.
            The name of the resulted message column is based on the alias of the provided aggCol!
        """  # noqa: E501

        if sendToDst is None:
            sendToDst = []
        if sendToSrc is None:
            sendToSrc = []

        # Back-compatibility workaround
        if not isinstance(aggCol, list):
            warnings.warn(
                "Passing single column to aggCol is deprecated, use list",
                DeprecationWarning,
            )
            return self.aggregateMessages(
                [aggCol], sendToSrc, sendToDst, intermediate_storage_level
            )
        if not isinstance(sendToSrc, list):
            warnings.warn(
                "Passing single column to sendToSrc is deprecated, use list",
                DeprecationWarning,
            )
            return self.aggregateMessages(
                aggCol, [sendToSrc], sendToDst, intermediate_storage_level
            )
        if not isinstance(sendToDst, list):
            warnings.warn(
                "Passing single column to sendToDst is deprecated, use list",
                DeprecationWarning,
            )
            return self.aggregateMessages(
                aggCol, sendToSrc, [sendToDst], intermediate_storage_level
            )

        if len(aggCol) == 0:
            raise TypeError("At least one aggregation column should be provided!")

        if (len(sendToSrc) == 0) and (len(sendToDst) == 0):
            raise ValueError(
                "Either `sendToSrc`, `sendToDst`, or both have to be provided"
            )
        return self._impl.aggregateMessages(
            aggCol=aggCol,
            sendToSrc=sendToSrc,
            sendToDst=sendToDst,
            intermediate_storage_level=intermediate_storage_level,
        )

    # Standard algorithms

    def connectedComponents(
        self,
        algorithm: str = "graphframes",
        checkpointInterval: int = 2,
        broadcastThreshold: int = 1000000,
        useLabelsAsComponents: bool = False,
        use_local_checkpoints: bool = False,
        max_iter: int = 2 ^ 31 - 2,
        storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK_DESER,
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
            use_local_checkpoints=use_local_checkpoints,
            max_iter=max_iter,
            storage_level=storage_level,
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
        sourceId: Any | None = None,
        maxIter: int | None = None,
        tol: float | None = None,
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
        sourceIds: list[Any] | None = None,
        maxIter: int | None = None,
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
        self, k: int, maxIter: int, weightCol: str | None = None
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

    def validate(
        self,
        check_vertices: bool = True,
        intermediate_storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK_DESER,
    ) -> None:
        """
        Validates the consistency and integrity of a graph by performing checks on the vertices and
        edges.

        :param check_vertices: a flag to indicate whether additional vertex consistency checks
            should be performed. If true, the method will verify that all vertices in the vertex
            DataFrame are represented in the edge DataFrame and vice versa. It is slow on big graphs.
        :param intermediate_storage_level: the storage level to be used when persisting
            intermediate DataFrame computations during the validation process.
        :return: Unit, as the method performs validation checks and throws an exception if
            validation fails.
        :raises ValueError: if there are any inconsistencies in the graph, such as duplicate
            vertices, mismatched vertices between edges and vertex DataFrames or missing
            connections.
        """
        persisted_vertices = self.vertices.persist(intermediate_storage_level)
        row = persisted_vertices.select(F.count_distinct(F.col(ID))).first()
        assert row is not None  # for type checker
        count_distinct_vertices = row[0]
        assert isinstance(count_distinct_vertices, int)  # for type checker
        total_count_vertices = persisted_vertices.count()
        if count_distinct_vertices != total_count_vertices:
            raise ValueError(
                f"Graph contains ({total_count_vertices - count_distinct_vertices}) duplicate vertices."
            )
        if check_vertices:
            vertices_set_from_edges = (
                self.edges.select(F.col(SRC).alias(ID))
                .union(self.edges.select(F.col(DST).alias(ID)))
                .distinct()
                .persist(intermediate_storage_level)
            )
            count_vertices_from_edges = vertices_set_from_edges.count()
            if count_vertices_from_edges > count_distinct_vertices:
                raise ValueError(
                    f"Graph is inconsistent: edges has {count_vertices_from_edges} "
                    + f"vertices, but vertices has {count_distinct_vertices} vertices."
                )

            combined = vertices_set_from_edges.join(self.vertices, ID, "left_anti")
            count_of_bad_vertices = combined.count()
            if count_of_bad_vertices > 0:
                raise ValueError(
                    "Vertices DataFrame does not contain all edges src/dst. "
                    + f"Found {count_of_bad_vertices} edges src/dst that are not in the vertices DataFrame."
                )
            _ = persisted_vertices.unpersist()
            _ = vertices_set_from_edges.unpersist()

    def as_undirected(self) -> "GraphFrame":
        """
        Converts the directed graph into an undirected graph by ensuring that all directed edges are
        bidirectional. For every directed edge (src, dst), a corresponding edge (dst, src) is added.

        :return: A new GraphFrame representing the undirected graph.
        """

        edge_attr_columns = [c for c in self.edges.columns if c not in [SRC, DST]]

        # Create the undirected edges by duplicating each edge in both directions
        forward_edges = self.edges.select(
            F.col(SRC), F.col(DST), F.struct(*edge_attr_columns).alias(EDGE)
        )
        backward_edges = self.edges.select(
            F.col(DST).alias(SRC),
            F.col(SRC).alias(DST),
            F.struct(*edge_attr_columns).alias(EDGE),
        )
        new_edges = forward_edges.union(backward_edges).select(SRC, DST, EDGE)

        # Preserve additional edge attributes
        edge_columns = [F.col(EDGE).getField(c).alias(c) for c in edge_attr_columns]

        # Select all columns including the new edge attributes
        selected_columns = [F.col(SRC), F.col(DST)] + edge_columns
        new_edges = new_edges.select(*selected_columns)

        return GraphFrame(self.vertices, new_edges)
