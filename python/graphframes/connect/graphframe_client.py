from __future__ import annotations

from typing import Self

from pyspark.sql.connect import functions as F
from pyspark.sql.connect import proto
from pyspark.sql.connect.client import SparkConnectClient
from pyspark.sql.connect.column import Column
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.plan import LogicalPlan
from pyspark.storagelevel import StorageLevel

from .proto import graphframes_pb2 as pb
from .utils import dataframe_to_proto, make_column_or_expr, make_str_or_long_id


class PregelConnect:
    def __init__(self, graph: "GraphFrameConnect") -> None:
        self.graph = graph
        self._max_iter = 10
        self._checkpoint_interval = 2
        self._col_name = None
        self._initial_expr = None
        self._update_after_agg_msgs_expr = None
        self._send_msg_to_src = None
        self._send_msg_to_dst = None
        self._agg_msg = None

    def setMaxIter(self, value: int) -> Self:
        self._max_iter = value
        return self

    def setCheckpointInterval(self, value: int) -> Self:
        self._checkpoint_interval = value
        return self

    def withVertexColumn(
        self,
        colName: str,
        initialExpr: Column | str,
        updateAfterAggMsgsExpr: Column | str,
    ) -> Self:
        self._col_name = colName
        self._initial_expr = initialExpr
        self._update_after_agg_msgs_expr = updateAfterAggMsgsExpr
        return self

    def sendMsgToSrc(self, msgExpr: Column | str) -> Self:
        self._send_msg_to_src = msgExpr
        return self

    def sendMsgToDst(self, msgExpr: Column | str) -> Self:
        self._send_msg_to_dst = msgExpr
        return self

    def aggMsgs(self, aggExpr: Column) -> Self:
        self._agg_msg = aggExpr
        return self

    def run(self) -> DataFrame:
        class Pregel(LogicalPlan):
            def __init__(
                self,
                max_iter: int,
                checkpoint_interval: int,
                vertex_col_name: str,
                agg_msg: Column | str,
                send2dst: Column | str,
                send2src: Column | str,
                vertex_col_init: Column | str,
                vertex_col_upd: Column | str,
                vertices: DataFrame,
                edges: DataFrame,
            ) -> None:
                self.max_iter = max_iter
                self.checkpoint_interval = checkpoint_interval
                self.vertex_col_name = vertex_col_name
                self.agg_msg = agg_msg
                self.send2dst = send2dst
                self.send2src = send2src
                self.vertex_col_init = vertex_col_init
                self.vertex_col_upd = vertex_col_upd
                self.vertices = vertices
                self.edges = edges

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                plan = self._create_proto_relation()
                pregel = pb.Pregel(
                    agg_msgs=make_column_or_expr(self.agg_msg, session),
                    send_msg_to_dst=make_column_or_expr(self.send2dst, session),
                    send_msg_to_src=make_column_or_expr(self.send2src, session),
                    checkpoint_interval=self.checkpoint_interval,
                    max_iter=self.max_iter,
                    additional_col_name=self.vertex_col_name,
                    additional_col_initial=make_column_or_expr(
                        self.vertex_col_init, session
                    ),
                    additional_col_upd=make_column_or_expr(
                        self.vertex_col_upd, session
                    ),
                )
                pb_message = pb.GraphFramesAPI(
                    vertices=dataframe_to_proto(self.vertices, session),
                    edges=dataframe_to_proto(self.edges, session),
                )
                pb_message.pregel = pregel
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

        if self._send_msg_to_src is None:
            raise ValueError("Send-to-src column is not initialized!")

        if self._send_msg_to_dst is None:
            raise ValueError("Send-to-dst column is not initialized!")

        return DataFrame.withPlan(
            Pregel(
                max_iter=self._max_iter,
                checkpoint_interval=self._checkpoint_interval,
                vertex_col_name=self._col_name,
                vertex_col_init=self._initial_expr,
                vertex_col_upd=self._update_after_agg_msgs_expr,
                agg_msg=self._agg_msg,
                send2dst=self._send_msg_to_dst,
                send2src=self._send_msg_to_src,
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
                "Source vertex ID column {} missing from edge DataFrame, which has columns: {}".format(
                    self.SRC, ",".join(e.columns)
                )
            )
        if self.DST not in e.columns:
            raise ValueError(
                "Destination vertex ID column {} missing from edge DataFrame, which has columns: {}".format(
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

    def cache(self) -> Self:
        self._vertices = self._vertices.cache()
        self._edges = self._edges.cache()
        return self

    def persist(self, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY) -> Self:
        self._vertices = self._vertices.persist(storageLevel=storageLevel)
        self._edges = self._edges.persist(storageLevel=storageLevel)
        return self

    def unpersist(self, blocking: bool = False) -> Self:
        self._vertices = self._vertices.unpersist(blocking=blocking)
        self._edges = self._edges.unpersist(blocking=blocking)
        return self

    @property
    def outDegrees(self) -> DataFrame:
        class OutDegrees(LogicalPlan):
            def __init__(self, v: DataFrame, e: DataFrame) -> None:
                self.v = v
                self.e = e

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.out_degrees = pb.OutDegrees()
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        return DataFrame.withPlan(OutDegrees(self._vertices, self._edges), self._spark)

    @property
    def inDegrees(self) -> DataFrame:
        class InDegrees(LogicalPlan):
            def __init__(self, v: DataFrame, e: DataFrame) -> None:
                self.v = v
                self.e = e

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.in_degrees = pb.InDegrees()
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        return DataFrame.withPlan(InDegrees(self._vertices, self._edges), self._spark)

    @property
    def degrees(self) -> DataFrame:
        class Degrees(LogicalPlan):
            def __init__(self, v: DataFrame, e: DataFrame) -> None:
                self.v = v
                self.e = e

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.degrees = pb.Degrees()
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        return DataFrame.withPlan(Degrees(self._vertices, self._edges), self._spark)

    @property
    def triplets(self) -> DataFrame:
        class Triplets(LogicalPlan):
            def __init__(self, v: DataFrame, e: DataFrame) -> None:
                self.v = v
                self.e = e

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.triplets = pb.Triplets()
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        return DataFrame.withPlan(Triplets(self._vertices, self._edges), self._spark)

    @property
    def pregel(self):
        return PregelConnect(self)

    def find(self, pattern: str) -> DataFrame:
        class Find(LogicalPlan):
            def __init__(self, v: DataFrame, e: DataFrame, pattern: str) -> None:
                self.v = v
                self.e = e
                self.p = pattern

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.find = pb.Find(pattern=self.p)
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        return DataFrame.withPlan(
            Find(self._vertices, self._edges, pattern), self._spark
        )

    def filterVertices(self, condition: str | Column) -> Self:
        class FilterVertices(LogicalPlan):
            def __init__(
                self, v: DataFrame, e: DataFrame, condition: str | Column
            ) -> None:
                self.v = v
                self.e = e
                self.c = condition

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                col_or_expr = make_column_or_expr(self.c, session)
                graphframes_api_call.filter_vertices = pb.FilterVertices(
                    condition=col_or_expr
                )
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        self._vertices = DataFrame.withPlan(
            FilterVertices(self._vertices, self._edges, condition), self._spark
        )
        self._edges = self._edges.join(
            self._vertices.withColumn(self.SRC, F.col(self.ID)),
            on=[self.SRC],
            how="left_semi",
        ).join(
            self._vertices.withColumn(self.DST, F.col(self.ID)),
            on=[self.DST],
            how="left_semi",
        )
        return self

    def filterEdges(self, condition: str | Column) -> Self:
        class FilterEdges(LogicalPlan):
            def __init__(
                self, v: DataFrame, e: DataFrame, condition: str | Column
            ) -> None:
                self.v = v
                self.e = e
                self.c = condition

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                col_or_expr = make_column_or_expr(self.c, session)
                graphframes_api_call.filter_edges = pb.FilterEdges(
                    condition=col_or_expr
                )
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        self._edges = DataFrame.withPlan(
            FilterEdges(self._vertices, self._edges, condition), self._spark
        )
        return self

    def dropIsolatedVertices(self) -> Self:
        class DropIsolatedVertices(LogicalPlan):
            def __init__(self, v: DataFrame, e: DataFrame) -> None:
                self.v = v
                self.e = e

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.drop_isolated_vertices = pb.DropIsolatedVertices()
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        self._vertices = DataFrame.withPlan(
            DropIsolatedVertices(self._vertices, self._edges), self._spark
        )
        return self

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
                graphframes_api_call.bfs = pb.BFS(
                    from_expr=make_column_or_expr(self.from_expr, session),
                    to_expr=make_column_or_expr(self.to_expr, session),
                    edge_filter=make_column_or_expr(self.edge_filter, session),
                    max_path_length=self.max_path_len,
                )
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        if edgeFilter is None:
            edgeFilter = F.lit(True)

        return DataFrame.withPlan(
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
        aggCol: Column | str,
        sendToSrc: Column | str | None = None,
        sendToDst: Column | str | None = None,
    ) -> DataFrame:
        class AggregateMessages(LogicalPlan):
            def __init__(
                self,
                v: DataFrame,
                e: DataFrame,
                agg_col: Column | str,
                send2src: Column | str | None,
                send2dst: Column | str | None,
            ) -> None:
                self.v = v
                self.e = e
                self.agg_col = agg_col
                self.send2src = send2src
                self.send2dst = send2dst

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.aggregate_messages = pb.AggregateMessages(
                    agg_col=make_column_or_expr(self.agg_col, session),
                    send_to_src=None
                    if self.send2src is None
                    else make_column_or_expr(self.send2src, session),
                    send_to_dst=None
                    if self.send2dst is None
                    else make_column_or_expr(self.send2dst, session),
                )
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        if sendToSrc is None and sendToDst is None:
            raise ValueError(
                "Either `sendToSrc`, `sendToDst`, or both have to be provided"
            )

        return DataFrame.withPlan(
            AggregateMessages(
                self._vertices, self._edges, aggCol, sendToSrc, sendToDst
            ),
            self._spark,
        )

    def connectedComponents(
        self,
        algorithm: str = "graphframes",
        checkpointInterval: int = 2,
        broadcastThreshold: int = 1000000,
    ) -> DataFrame:
        class ConnectedComponents(LogicalPlan):
            def __init__(
                self,
                v: DataFrame,
                e: DataFrame,
                algorithm: str,
                checkpoint_interval: int,
                broadcast_threshold: int,
            ) -> None:
                self.v = v
                self.e = e
                self.algorithm = algorithm
                self.checkpoint_interval = checkpoint_interval
                self.broadcast_threshold = broadcast_threshold

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.connected_components = pb.ConnectedComponents(
                    algorithm=self.algorithm,
                    checkpoint_interval=self.checkpoint_interval,
                    broadcast_threshold=self.broadcast_threshold,
                )
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        return DataFrame.withPlan(
            ConnectedComponents(
                self._vertices,
                self._edges,
                algorithm,
                checkpointInterval,
                broadcastThreshold,
            ),
            self._spark,
        )

    def labelPropagation(self, maxIter: int) -> DataFrame:
        class LabelPropagation(LogicalPlan):
            def __init__(self, v: DataFrame, e: DataFrame, max_iter: int) -> None:
                self.v = v
                self.e = e
                self.max_iter = max_iter

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.label_propagation = pb.LabelPropagation(
                    max_iter=self.max_iter
                )
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        return DataFrame.withPlan(
            LabelPropagation(self._vertices, self._edges, maxIter), self._spark
        )

    def pageRank(
        self,
        resetProbability: float = 0.15,
        sourceId: Optional[Any] = None,
        maxIter: Optional[int] = None,
        tol: Optional[float] = None,
    ) -> "GraphFrame":
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
        return _from_java_gf(jgf, self._spark)

    def parallelPersonalizedPageRank(
        self,
        resetProbability: float = 0.15,
        sourceIds: Optional[list[Any]] = None,
        maxIter: Optional[int] = None,
    ) -> "GraphFrame":
        assert (
            sourceIds is not None and len(sourceIds) > 0
        ), "Source vertices Ids sourceIds must be provided"
        assert maxIter is not None, "Max number of iterations maxIter must be provided"
        sourceIds = self._sc._jvm.PythonUtils.toArray(sourceIds)
        builder = self._jvm_graph.parallelPersonalizedPageRank()
        builder = builder.resetProbability(resetProbability)
        builder = builder.sourceIds(sourceIds)
        builder = builder.maxIter(maxIter)
        jgf = builder.run()
        return _from_java_gf(jgf, self._spark)

    def shortestPaths(self, landmarks: list[str | int]) -> DataFrame:
        class ShortestPaths(LogicalPlan):
            def __init__(
                self, v: DataFrame, e: DataFrame, landmarks: list[str | int]
            ) -> None:
                self.v = v
                self.e = e
                self.landmarks = landmarks

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.shortest_paths = pb.ShortestPaths(
                    landmarks=[make_str_or_long_id(raw_id) for raw_id in self.landmarks]
                )
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        return DataFrame.withPlan(
            ShortestPaths(self._vertices, self._edges, landmarks), self._spark
        )

    def stronglyConnectedComponents(self, maxIter: int) -> DataFrame:
        class StronglyConnectedComponents(LogicalPlan):
            def __init__(self, v: DataFrame, e: DataFrame, max_iter: int) -> None:
                self.v = v
                self.e = e
                self.max_iter = max_iter

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.strongly_connected_components = (
                    pb.StronglyConnectedComponents(max_iter=self.max_iter)
                )
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        return DataFrame.withPlan(
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
                graphframes_api_call.svd_plus_plus = pb.SVDPlusPlus(
                    rank=self.rank,
                    max_iter=self.max_iter,
                    min_value=self.min_value,
                    max_value=self.max_value,
                    gamma1=self.gamma1,
                    gamma2=self.gamma2,
                    gamma6=self.gamma6,
                    gamma7=self.gamma7,
                )
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        output = DataFrame.withPlan(
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
                self.v = v
                self.e = e

            def plan(self, session: SparkConnectClient) -> proto.Relation:
                graphframes_api_call = GraphFrameConnect._get_pb_api_message(
                    self.v, self.e, session
                )
                graphframes_api_call.triangle_count = pb.TriangleCount()
                plan = self._create_proto_relation()
                plan.extension.Pack(graphframes_api_call)
                return plan

        return DataFrame.withPlan(
            TriangleCount(self._vertices, self._edges), self._spark
        )
