from __future__ import annotations

from pyspark.sql.connect import functions as F
from pyspark.sql.connect import proto
from pyspark.sql.connect.client import SparkConnectClient
from pyspark.sql.connect.column import Column
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.plan import LogicalPlan
from pyspark.sql.connect.session import SparkSession
from pyspark.storagelevel import StorageLevel

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

from .proto import graphframes_pb2 as pb
from .utils import (
    dataframe_to_proto,
    make_column_or_expr,
    make_str_or_long_id,
    storage_level_to_proto,
)


# Spark 4 removed the withPlan method in favor of the constructor, but Spark 3
# does not have the plan as an arg in the constructor, so we need to handle
# both cases.
def _dataframe_from_plan(plan: LogicalPlan, session: SparkSession) -> DataFrame:
    if hasattr(DataFrame, "withPlan"):
        # Spark 3
        return DataFrame.withPlan(plan, session)

    # Spark 4
    return DataFrame(plan, session)


class PregelConnect:
    """Implements a Pregel-like bulk-synchronous message-passing API based on DataFrame operations.

    See `Malewicz et al., Pregel: a system for large-scale graph processing <https://doi.org/10.1145/1807167.1807184>`_
    for a detailed description of the Pregel algorithm.

    You can construct a Pregel instance using either this constructor or :attr:`graphframes.GraphFrame.pregel`,
    then use builder pattern to describe the operations, and then call :func:`run` to start a run.
    It returns a DataFrame of vertices from the last iteration.

    When a run starts, it expands the vertices DataFrame using column expressions defined by :func:`withVertexColumn`.
    Those additional vertex properties can be changed during Pregel iterations.
    In each Pregel iteration, there are three phases:
      - Given each edge triplet, generate messages and specify target vertices to send,
        described by :func:`sendMsgToDst` and :func:`sendMsgToSrc`.
      - Aggregate messages by target vertex IDs, described by :func:`aggMsgs`.
      - Update additional vertex properties based on aggregated messages and states from previous iteration,
        described by :func:`withVertexColumn`.

    Please find what columns you can reference at each phase in the method API docs.

    You can control the number of iterations by :func:`setMaxIter` and check API docs for advanced controls.

    :param graph: a :class:`graphframes.GraphFrame` object holding a graph with vertices and edges stored as DataFrames.
    """  # noqa: E501

    def __init__(self, graph: "GraphFrameConnect") -> None:
        self.graph = graph
        self._max_iter = 10
        self._checkpoint_interval = 2
        self._col_name = None
        self._initial_expr = None
        self._update_after_agg_msgs_expr = None
        self._send_msg_to_src = []
        self._send_msg_to_dst = []
        self._agg_msg = None
        self._early_stopping = False
        self._use_local_checkpoints = False
        self._storage_level = StorageLevel.MEMORY_AND_DISK_DESER
        self._initial_active_expr: Column | str | None = None
        self._update_active_expr: Column | str | None = None
        self._stop_if_all_non_active = False
        self._skip_messages_from_non_active = False

    def setMaxIter(self, value: int) -> Self:
        """Sets the max number of iterations (default: 2)."""
        self._max_iter = value
        return self

    def setCheckpointInterval(self, value: int) -> Self:
        """Sets the number of iterations between two checkpoints (default: 2).

        This is an advanced control to balance query plan optimization and checkpoint data I/O cost.
        In most cases, you should keep the default value.

        Checkpoint is disabled if this is set to 0.
        """
        self._checkpoint_interval = value
        return self

    def setEarlyStopping(self, value: bool) -> Self:
        """Set should Pregel stop earlier in case of no new messages to send or not.

        Early stopping allows to terminate Pregel before reaching maxIter by checking if there are any non-null messages.
        While in some cases it may gain significant performance boost, in other cases it can lead to performance degradation,
        because checking if the messages DataFrame is empty or not is an action and requires materialization of the Spark Plan
        with some additional computations.

        In the case when the user can assume a good value of maxIter, it is recommended to leave this value to the default "false".
        In the case when it is hard to estimate the number of iterations required for convergence,
        it is recommended to set this value to "false" to avoid iterating over convergence until reaching maxIter.
        When this value is "true", maxIter can be set to a bigger value without risks.
        """  # noqa: E501
        self._early_stopping = value
        return self

    def withVertexColumn(
        self,
        colName: str,
        initialExpr: Column | str,
        updateAfterAggMsgsExpr: Column | str,
    ) -> Self:
        """Defines an additional vertex column at the start of run and how to update it in each iteration.

        You can call it multiple times to add more than one additional vertex columns.

        :param colName: the name of the additional vertex column.
                        It cannot be an existing vertex column in the graph.
        :param initialExpr: the expression to initialize the additional vertex column.
                            You can reference all original vertex columns in this expression.
        :param updateAfterAggMsgsExpr: the expression to update the additional vertex column after messages aggregation.
                                       You can reference all original vertex columns, additional vertex columns, and the
                                       aggregated message column using :func:`msg`.
                                       If the vertex received no messages, the message column would be null.
        """  # noqa: E501
        self._col_name = colName
        self._initial_expr = initialExpr
        self._update_after_agg_msgs_expr = updateAfterAggMsgsExpr
        return self

    def sendMsgToSrc(self, msgExpr: Column | str) -> Self:
        """Defines a message to send to the source vertex of each edge triplet.

        You can call it multiple times to send more than one messages.

        See method :func:`sendMsgToDst`.

        :param msgExpr: the expression of the message to send to the source vertex given a (src, edge, dst) triplet.
                        Source/destination vertex properties and edge properties are nested under columns `src`, `dst`,
                        and `edge`, respectively.
                        You can reference them using :func:`src`, :func:`dst`, and :func:`edge`.
                        Null messages are not included in message aggregation.
        """  # noqa: E501
        self._send_msg_to_src.append(msgExpr)
        return self

    def sendMsgToDst(self, msgExpr: Column | str) -> Self:
        """Defines a message to send to the destination vertex of each edge triplet.

        You can call it multiple times to send more than one messages.

        See method :func:`sendMsgToSrc`.

        :param msgExpr: the message expression to send to the destination vertex given a (`src`, `edge`, `dst`) triplet.
                        Source/destination vertex properties and edge properties are nested under columns `src`, `dst`,
                        and `edge`, respectively.
                        You can reference them using :func:`src`, :func:`dst`, and :func:`edge`.
                        Null messages are not included in message aggregation.
        """  # noqa: E501
        self._send_msg_to_dst.append(msgExpr)
        return self

    def aggMsgs(self, aggExpr: Column) -> Self:
        """Defines how messages are aggregated after grouped by target vertex IDs.

        :param aggExpr: the message aggregation expression, such as `sum(Pregel.msg())`.
                        You can reference the message column by :func:`msg` and the vertex ID by `col("id")`,
                        while the latter is usually not used.
        """  # noqa: E501
        self._agg_msg = aggExpr
        return self

    def setStopIfAllNonActiveVertices(self, value: bool) -> Self:
        """Set should Pregel stop if all the vertices voted to halt.

        Activity (or vote) is determined based on the activity_col.
        See methods :func:`setInitialActiveVertexExpression` and :func:`setUpdateActiveVertexExpression` for details
        how to set and update activity_col.

        Be aware that checking of the vote is not free but a Spark Action. In case the
        condition is not realistically reachable but set, it will just slow down the algorithm.

        :param value: the boolean value.
        """  # noqa: E501
        self._stop_if_all_non_active = value
        return self

    def setInitialActiveVertexExpression(self, value: Column | str) -> Self:
        """Sets the initial expression for the active vertex column.

        The active vertex column is used to determine if a vertices voting result on each iteration of Pregel.
        This expression is evaluated on the initial vertices DataFrame to set the initial state of the activity column.

        :param value: expression to compute the initial active state of vertices.
                      You can reference all original vertex columns in this expression.
        """  # noqa: E501
        self._initial_active_expr = value
        return self

    def setUpdateActiveVertexExpression(self, value: Column | str) -> Self:
        """Sets the expression to update the active vertex column.

        The active vertex column is used to determine if a vertices voting result on each iteration of Pregel.
        This expression is evaluated on the updated vertices DataFrame to set the new state of the activity column.

        :param value: expression to compute the new active state of vertices.
                      You can reference all original vertex columns and additional vertex columns in this expression.
        """  # noqa: E501
        self._update_active_expr = value
        return self

    def setSkipMessagesFromNonActiveVertices(self, value: bool) -> Self:
        """Set should Pregel skip sending messages from non-active vertices.

        When this option is enabled, messages will not be sent from vertices that are marked as inactive.
        This can help optimize performance by avoiding unnecessary message propagation from inactive vertices.

        :param value: boolean value.
        """  # noqa: E501
        self._skip_messages_from_non_active = value
        return self

    def setUseLocalCheckpoints(self, value: bool) -> Self:
        """Set should Pregel use local checkpoints.

        Local checkpoints are faster and do not require configuring a persistent storage.
        At the same time, local checkpoints are less reliable and may create a big load on local disks of executors.

        :param value: boolean value.
        """  # noqa: E501
        self._use_local_checkpoints = value
        return self

    def setIntermediateStorageLevel(self, storage_level: StorageLevel) -> Self:
        """Set the intermediate storage level.
        On each iteration, Pregel cache results with a requested storage level.

        For very big graphs it is recommended to use DISK_ONLY.

        :param storage_level: storage level to use.
        """  # noqa: E501
        self._storage_level = storage_level
        return self

    def run(self) -> DataFrame:
        class Pregel(LogicalPlan):
            def __init__(
                self,
                max_iter: int,
                checkpoint_interval: int,
                early_stopping: bool,
                vertex_col_name: str,
                agg_msg: Column | str,
                send2dst: list[Column | str],
                send2src: list[Column | str],
                vertex_col_init: Column | str,
                vertex_col_upd: Column | str,
                use_local_checkpoints: bool,
                storage_level: StorageLevel,
                initial_active_col: Column | str | None,
                update_active_col: Column | str | None,
                stop_if_all_non_active: bool,
                skip_message_from_non_active: bool,
                vertices: DataFrame,
                edges: DataFrame,
            ) -> None:
                super().__init__(None)
                self.max_iter = max_iter
                self.checkpoint_interval = checkpoint_interval
                self.early_stopping = early_stopping
                self.vertex_col_name = vertex_col_name
                self.agg_msg = agg_msg
                self.send2dst = send2dst
                self.send2src = send2src
                self.vertex_col_init = vertex_col_init
                self.vertex_col_upd = vertex_col_upd
                self.use_local_checkpoints = use_local_checkpoints
                self.storage_level = storage_level
                self.initial_active_expr = initial_active_col
                self.update_active_expr = update_active_col
                self.stop_if_all_non_active = stop_if_all_non_active
                self.skip_message_from_non_active = skip_message_from_non_active
                self.vertices = vertices
                self.edges = edges

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                pregel = pb.Pregel(
                    agg_msgs=make_column_or_expr(self.agg_msg, session),
                    send_msg_to_dst=[
                        make_column_or_expr(c_or_e, session) for c_or_e in self.send2dst
                    ],
                    send_msg_to_src=[
                        make_column_or_expr(c_or_e, session) for c_or_e in self.send2src
                    ],
                    checkpoint_interval=self.checkpoint_interval,
                    max_iter=self.max_iter,
                    additional_col_name=self.vertex_col_name,
                    additional_col_initial=make_column_or_expr(self.vertex_col_init, session),
                    additional_col_upd=make_column_or_expr(self.vertex_col_upd, session),
                    early_stopping=self.early_stopping,
                    use_local_checkpoints=self.use_local_checkpoints,
                    storage_level=storage_level_to_proto(self.storage_level),
                    stop_if_all_non_active=self.stop_if_all_non_active,
                    skip_messages_from_non_active=self.skip_message_from_non_active,
                    initial_active_expr=make_column_or_expr(self.initial_active_expr, session)
                    if self.initial_active_expr is not None
                    else None,
                    update_active_expr=make_column_or_expr(self.update_active_expr, session)
                    if self.update_active_expr is not None
                    else None,
                )
                pb_message = pb.GraphFramesAPI(
                    vertices=dataframe_to_proto(self.vertices, session),
                    edges=dataframe_to_proto(self.edges, session),
                )
                pb_message.pregel.CopyFrom(pregel)
                plan = self._create_proto_relation()
                plan.extension.Pack(pb_message)
                return plan

        if (
            (self._col_name is None)
            or (self._initial_expr is None)
            or (self._update_after_agg_msgs_expr is None)
        ):
            raise ValueError("Initial vertex column is not initialized!")

        if self._agg_msg is None:
            raise ValueError("AggMsg is not initialized!")

        return _dataframe_from_plan(
            Pregel(
                max_iter=self._max_iter,
                checkpoint_interval=self._checkpoint_interval,
                vertex_col_name=self._col_name,
                vertex_col_init=self._initial_expr,
                vertex_col_upd=self._update_after_agg_msgs_expr,
                agg_msg=self._agg_msg,
                send2dst=self._send_msg_to_dst,
                send2src=self._send_msg_to_src,
                early_stopping=self._early_stopping,
                use_local_checkpoints=self._use_local_checkpoints,
                initial_active_col=self._initial_active_expr,
                update_active_col=self._update_active_expr,
                stop_if_all_non_active=self._stop_if_all_non_active,
                skip_message_from_non_active=self._skip_messages_from_non_active,
                storage_level=self._storage_level,
                vertices=self.graph._vertices,
                edges=self.graph._edges,
            ),
            session=self.graph._spark,
        )

    @staticmethod
    def msg() -> Column:
        return F.col("_pregel_msg_")

    @staticmethod
    def src(colName: str) -> Column:
        return F.col("src." + colName)

    @staticmethod
    def dst(colName: str) -> Column:
        return F.col("dst." + colName)

    @staticmethod
    def edge(colName: str) -> Column:
        return F.col("edge." + colName)


class GraphFrameConnect:
    ID = "id"
    SRC = "src"
    DST = "dst"
    EDGE = "edge"

    def __init__(self, v: DataFrame, e: DataFrame) -> None:
        self._vertices = v
        self._edges = e
        self._spark = v.sparkSession

        if self.ID not in v.columns:
            raise ValueError(
                "Vertex ID column {} missing from vertex DataFrame, which has columns: {}".format(
                    self.ID, ",".join(v.columns)
                )
            )
        if self.SRC not in e.columns:
            raise ValueError(
                "Source vertex ID column {} missing from edge DataFrame, which has columns: {}".format(  # noqa: E501
                    self.SRC, ",".join(e.columns)
                )
            )
        if self.DST not in e.columns:
            raise ValueError(
                "Destination vertex ID column {} missing from edge DataFrame, which has columns: {}".format(  # noqa: E501
                    self.DST, ",".join(e.columns)
                )
            )

    @staticmethod
    def _get_pb_api_message(
        vertices: DataFrame, edges: DataFrame, client: SparkConnectClient
    ) -> pb.GraphFramesAPI:
        return pb.GraphFramesAPI(
            vertices=dataframe_to_proto(vertices, client),
            edges=dataframe_to_proto(edges, client),
        )

    @property
    def vertices(self) -> DataFrame:
        return self._vertices

    @property
    def edges(self) -> DataFrame:
        return self._edges

    def __repr__(self) -> str:
        # Exactly like in the scala core
        v_cols = [self.ID] + [col for col in self.vertices.columns if col != self.ID]
        e_cols = [self.SRC, self.DST] + [
            col for col in self.edges.columns if col not in {self.SRC, self.DST}
        ]
        v = self.vertices.select(*v_cols).__repr__()
        e = self.edges.select(*e_cols).__repr__()

        return f"GraphFrame(v:{v}, e:{e})"

    def cache(self) -> "GraphFrameConnect":
        new_vertices = self._vertices.cache()
        new_edges = self._edges.cache()
        return GraphFrameConnect(new_vertices, new_edges)

    def persist(self, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY) -> "GraphFrameConnect":
        new_vertices = self._vertices.persist(storageLevel=storageLevel)
        new_edges = self._edges.persist(storageLevel=storageLevel)
        return GraphFrameConnect(new_vertices, new_edges)

    def unpersist(self, blocking: bool = False) -> "GraphFrameConnect":
        new_vertices = self._vertices.unpersist(blocking=blocking)
        new_edges = self._edges.unpersist(blocking=blocking)
        return GraphFrameConnect(new_vertices, new_edges)

    @property
    def outDegrees(self) -> DataFrame:
        return self._edges.groupBy(F.col(self.SRC).alias(self.ID)).agg(
            F.count("*").alias("outDegree")
        )

    @property
    def inDegrees(self) -> DataFrame:
        return self._edges.groupBy(F.col(self.DST).alias(self.ID)).agg(
            F.count("*").alias("inDegree")
        )

    @property
    def degrees(self) -> DataFrame:
        return (
            self._edges.select(F.explode(F.array(F.col(self.SRC), F.col(self.DST))).alias(self.ID))
            .groupBy(self.ID)
            .agg(F.count("*").alias("degree"))
        )

    @property
    def triplets(self) -> DataFrame:
        class Triplets(LogicalPlan):
            def __init__(self, v: DataFrame, e: DataFrame) -> None:
                super().__init__(None)
                self.v = v
                self.e = e

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.triplets.CopyFrom(pb.Triplets())
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        return _dataframe_from_plan(Triplets(self._vertices, self._edges), self._spark)

    @property
    def pregel(self):
        return PregelConnect(self)

    def find(self, pattern: str) -> DataFrame:
        class Find(LogicalPlan):
            def __init__(self, v: DataFrame, e: DataFrame, pattern: str) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.p = pattern

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.find.CopyFrom(pb.Find(pattern=self.p))
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        return _dataframe_from_plan(Find(self._vertices, self._edges, pattern), self._spark)

    def filterVertices(self, condition: str | Column) -> "GraphFrameConnect":
        class FilterVertices(LogicalPlan):
            def __init__(self, v: DataFrame, e: DataFrame, condition: str | Column) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.c = condition

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                col_or_expr = make_column_or_expr(self.c, session)
                graphframes_api_call.filter_vertices.CopyFrom(
                    pb.FilterVertices(condition=col_or_expr)
                )
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        new_vertices = _dataframe_from_plan(
            FilterVertices(self._vertices, self._edges, condition), self._spark
        )
        # Exactly like in the scala-core
        new_edges = self._edges.join(
            new_vertices.withColumn(self.SRC, F.col(self.ID)),
            on=[self.SRC],
            how="left_semi",
        ).join(
            new_vertices.withColumn(self.DST, F.col(self.ID)),
            on=[self.DST],
            how="left_semi",
        )
        return GraphFrameConnect(new_vertices, new_edges)

    def filterEdges(self, condition: str | Column) -> "GraphFrameConnect":
        class FilterEdges(LogicalPlan):
            def __init__(self, v: DataFrame, e: DataFrame, condition: str | Column) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.c = condition

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                col_or_expr = make_column_or_expr(self.c, session)
                graphframes_api_call.filter_edges.CopyFrom(pb.FilterEdges(condition=col_or_expr))
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        new_edges = _dataframe_from_plan(
            FilterEdges(self._vertices, self._edges, condition), self._spark
        )
        return GraphFrameConnect(self._vertices, new_edges)

    def dropIsolatedVertices(self) -> "GraphFrameConnect":
        class DropIsolatedVertices(LogicalPlan):
            def __init__(self, v: DataFrame, e: DataFrame) -> None:
                super().__init__(None)
                self.v = v
                self.e = e

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.drop_isolated_vertices.CopyFrom(pb.DropIsolatedVertices())
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        new_vertices = _dataframe_from_plan(
            DropIsolatedVertices(self._vertices, self._edges), self._spark
        )
        return GraphFrameConnect(new_vertices, self._edges)

    def bfs(
        self,
        fromExpr: Column | str,
        toExpr: Column | str,
        edgeFilter: Column | str | None = None,
        maxPathLength: int = 10,
    ) -> DataFrame:
        class BFS(LogicalPlan):
            def __init__(
                self,
                v: DataFrame,
                e: DataFrame,
                from_expr: Column | str,
                to_expr: Column | str,
                edge_filter: Column | str,
                max_path_len: int,
            ) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.from_expr = from_expr
                self.to_expr = to_expr
                self.edge_filter = edge_filter
                self.max_path_len = max_path_len

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.bfs.CopyFrom(
                    pb.BFS(
                        from_expr=make_column_or_expr(self.from_expr, session),
                        to_expr=make_column_or_expr(self.to_expr, session),
                        edge_filter=make_column_or_expr(self.edge_filter, session),
                        max_path_length=self.max_path_len,
                    )
                )
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        if edgeFilter is None:
            edgeFilter: Column = F.lit(True)

        return _dataframe_from_plan(
            BFS(
                v=self._vertices,
                e=self._edges,
                from_expr=fromExpr,
                to_expr=toExpr,
                edge_filter=edgeFilter,
                max_path_len=maxPathLength,
            ),
            self._spark,
        )

    def aggregateMessages(
        self,
        aggCol: list[Column | str],
        sendToSrc: list[Column | str],
        sendToDst: list[Column | str],
        intermediate_storage_level: StorageLevel,
    ) -> DataFrame:
        class AggregateMessages(LogicalPlan):
            def __init__(
                self,
                v: DataFrame,
                e: DataFrame,
                agg_col: list[Column | str],
                send2src: list[Column | str],
                send2dst: list[Column | str],
                storage_level: StorageLevel,
            ) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.agg_col = agg_col
                self.send2src = send2src
                self.send2dst = send2dst
                self.storage_level = storage_level

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.aggregate_messages.CopyFrom(
                    pb.AggregateMessages(
                        agg_col=[make_column_or_expr(x, session) for x in self.agg_col],
                        send_to_src=[make_column_or_expr(x, session) for x in self.send2src],
                        send_to_dst=[make_column_or_expr(x, session) for x in self.send2dst],
                        storage_level=storage_level_to_proto(self.storage_level),
                    )
                )
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        if sendToSrc is None and sendToDst is None:
            raise ValueError("Either `sendToSrc`, `sendToDst`, or both have to be provided")

        return _dataframe_from_plan(
            AggregateMessages(
                self._vertices,
                self._edges,
                aggCol,
                sendToSrc,
                sendToDst,
                intermediate_storage_level,
            ),
            self._spark,
        )

    def connectedComponents(
        self,
        algorithm: str = "graphframes",
        checkpointInterval: int = 2,
        broadcastThreshold: int = 1000000,
        useLabelsAsComponents: bool = False,
    ) -> DataFrame:
        class ConnectedComponents(LogicalPlan):
            def __init__(
                self,
                v: DataFrame,
                e: DataFrame,
                algorithm: str,
                checkpoint_interval: int,
                broadcast_threshold: int,
                use_labels_as_components: bool,
            ) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.algorithm = algorithm
                self.checkpoint_interval = checkpoint_interval
                self.broadcast_threshold = broadcast_threshold
                self.use_labels_as_components = use_labels_as_components

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.connected_components.CopyFrom(
                    pb.ConnectedComponents(
                        algorithm=self.algorithm,
                        checkpoint_interval=self.checkpoint_interval,
                        broadcast_threshold=self.broadcast_threshold,
                        use_labels_as_components=self.use_labels_as_components,
                    )
                )
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        return _dataframe_from_plan(
            ConnectedComponents(
                self._vertices,
                self._edges,
                algorithm,
                checkpointInterval,
                broadcastThreshold,
                useLabelsAsComponents,
            ),
            self._spark,
        )

    def labelPropagation(self, maxIter: int) -> DataFrame:
        class LabelPropagation(LogicalPlan):
            def __init__(self, v: DataFrame, e: DataFrame, max_iter: int) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.max_iter = max_iter

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.label_propagation.CopyFrom(
                    pb.LabelPropagation(max_iter=self.max_iter)
                )
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        return _dataframe_from_plan(
            LabelPropagation(self._vertices, self._edges, maxIter), self._spark
        )

    def _update_page_rank_edge_weights(self, new_vertices: DataFrame) -> "GraphFrameConnect":
        cols2select = self.edges.columns + ["weight"]
        new_edges = (
            self._edges.join(
                new_vertices.withColumn(self.SRC, F.col(self.ID)),
                on=[self.SRC],
                how="inner",
            )
            .join(
                self.outDegrees.withColumn(self.SRC, F.col(self.ID)),
                on=[self.SRC],
                how="inner",
            )
            .withColumn("weight", F.col("pagerank") / F.col("outDegree"))
            .select(*cols2select)
        )
        return GraphFrameConnect(new_vertices, new_edges)

    def pageRank(
        self,
        resetProbability: float = 0.15,
        sourceId: str | int | None = None,
        maxIter: int | None = None,
        tol: float | None = None,
    ) -> "GraphFrameConnect":
        class PageRank(LogicalPlan):
            def __init__(
                self,
                v: DataFrame,
                e: DataFrame,
                reset_prob: float,
                source_id: str | int | None,
                max_iter: int | None,
                tol: float | None,
            ) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.reset_prob = reset_prob
                self.source_id = source_id
                self.max_iter = max_iter
                self.tol = tol

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.page_rank.CopyFrom(
                    pb.PageRank(
                        reset_probability=self.reset_prob,
                        source_id=(
                            None if self.source_id is None else make_str_or_long_id(self.source_id)
                        ),
                        max_iter=self.max_iter,
                        tol=self.tol,
                    )
                )
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        if (maxIter is None) == (tol is None):
            # TODO: in classic it is not an axception but assert;
            # at the same time I think it should be an exception.
            raise ValueError("Exactly one of maxIter or tol should be set.")

        new_vertices = _dataframe_from_plan(
            PageRank(
                self._vertices,
                self._edges,
                reset_prob=resetProbability,
                source_id=sourceId,
                max_iter=maxIter,
                tol=tol,
            ),
            self._spark,
        )
        # TODO: should this part to be optional? Like 'compute_edge_weights'?
        return self._update_page_rank_edge_weights(new_vertices)

    def parallelPersonalizedPageRank(
        self,
        resetProbability: float = 0.15,
        sourceIds: list[str | int] | None = None,
        maxIter: int | None = None,
    ) -> "GraphFrameConnect":
        class ParallelPersonalizedPageRank(LogicalPlan):
            def __init__(
                self,
                v: DataFrame,
                e: DataFrame,
                reset_prob: float,
                source_ids: list[str | int],
                max_iter: int,
            ) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.reset_prob = reset_prob
                self.source_ids = source_ids
                self.max_iter = max_iter

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.parallel_personalized_page_rank.CopyFrom(
                    pb.ParallelPersonalizedPageRank(
                        reset_probability=self.reset_prob,
                        source_ids=[make_str_or_long_id(raw_id) for raw_id in self.source_ids],
                        max_iter=self.max_iter,
                    )
                )
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        assert (
            sourceIds is not None and len(sourceIds) > 0
        ), "Source vertices Ids sourceIds must be provided"
        assert maxIter is not None, "Max number of iterations maxIter must be provided"

        new_vertices = _dataframe_from_plan(
            ParallelPersonalizedPageRank(
                self._vertices,
                self._edges,
                reset_prob=resetProbability,
                source_ids=sourceIds,
                max_iter=maxIter,
            ),
            self._spark,
        )
        return self._update_page_rank_edge_weights(new_vertices)

    def powerIterationClustering(
        self, k: int, maxIter: int, weightCol: str | None = None
    ) -> DataFrame:
        class PowerIterationClustering(LogicalPlan):
            def __init__(
                self,
                v: DataFrame,
                e: DataFrame,
                k: int,
                max_iter: int,
                weight_col: str | None,
            ) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.k = k
                self.max_iter = max_iter
                self.weight_col = weight_col

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.power_iteration_clustering.CopyFrom(
                    pb.PowerIterationClustering(
                        k=self.k,
                        max_iter=self.max_iter,
                        weight_col=self.weight_col,
                    )
                )
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        return _dataframe_from_plan(
            PowerIterationClustering(self._vertices, self._edges, k, maxIter, weightCol),
            self._spark,
        )

    def shortestPaths(self, landmarks: list[str | int]) -> DataFrame:
        class ShortestPaths(LogicalPlan):
            def __init__(self, v: DataFrame, e: DataFrame, landmarks: list[str | int]) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.landmarks = landmarks

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.shortest_paths.CopyFrom(
                    pb.ShortestPaths(
                        landmarks=[make_str_or_long_id(raw_id) for raw_id in self.landmarks]
                    )
                )
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        return _dataframe_from_plan(
            ShortestPaths(self._vertices, self._edges, landmarks), self._spark
        )

    def stronglyConnectedComponents(self, maxIter: int) -> DataFrame:
        class StronglyConnectedComponents(LogicalPlan):
            def __init__(self, v: DataFrame, e: DataFrame, max_iter: int) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.max_iter = max_iter

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.strongly_connected_components.CopyFrom(
                    pb.StronglyConnectedComponents(max_iter=self.max_iter)
                )
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        return _dataframe_from_plan(
            StronglyConnectedComponents(self._vertices, self._edges, maxIter),
            self._spark,
        )

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
        return_loss: bool = False,  # TODO: should it be True to mimic the classic API?
    ) -> tuple[DataFrame, float]:
        class SVDPlusPlus(LogicalPlan):
            def __init__(
                self,
                v: DataFrame,
                e: DataFrame,
                rank: int,
                max_iter: int,
                min_value: float,
                max_value: float,
                gamma1: float,
                gamma2: float,
                gamma6: float,
                gamma7: float,
            ) -> None:
                super().__init__(None)
                self.v = v
                self.e = e
                self.rank = rank
                self.max_iter = max_iter
                self.min_value = min_value
                self.max_value = max_value
                self.gamma1 = gamma1
                self.gamma2 = gamma2
                self.gamma6 = gamma6
                self.gamma7 = gamma7

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.svd_plus_plus.CopyFrom(
                    pb.SVDPlusPlus(
                        rank=self.rank,
                        max_iter=self.max_iter,
                        min_value=self.min_value,
                        max_value=self.max_value,
                        gamma1=self.gamma1,
                        gamma2=self.gamma2,
                        gamma6=self.gamma6,
                        gamma7=self.gamma7,
                    )
                )
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        output = _dataframe_from_plan(
            SVDPlusPlus(
                self._vertices,
                self._edges,
                rank=rank,
                max_iter=maxIter,
                min_value=minValue,
                max_value=maxValue,
                gamma1=gamma1,
                gamma2=gamma2,
                gamma6=gamma6,
                gamma7=gamma7,
            ),
            self._spark,
        )

        if return_loss:
            # This branch may be computationaly expensive and it is not lazy!
            return (output.drop("loss"), output.select("loss").take(1)[0]["loss"])
        else:
            return (output.drop("loss"), -1.0)

    def triangleCount(self) -> DataFrame:
        class TriangleCount(LogicalPlan):
            def __init__(self, v: DataFrame, e: DataFrame) -> None:
                super().__init__(None)
                self.v = v
                self.e = e

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.triangle_count.CopyFrom(pb.TriangleCount())
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        return _dataframe_from_plan(TriangleCount(self._vertices, self._edges), self._spark)
