from typing import ClassVar as _ClassVar
from typing import Iterable as _Iterable
from typing import Mapping as _Mapping
from typing import Optional as _Optional
from typing import Union as _Union

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf.internal import containers as _containers

DESCRIPTOR: _descriptor.FileDescriptor

class GraphFramesAPI(_message.Message):
    __slots__ = (
        "vertices",
        "edges",
        "aggregate_messages",
        "bfs",
        "connected_components",
        "degrees",
        "drop_isolated_vertices",
        "filter_edges",
        "filter_vertices",
        "find",
        "in_degrees",
        "label_propagation",
        "out_degrees",
        "page_rank",
        "parallel_personalized_page_rank",
        "pregel",
        "shortest_paths",
        "strongly_connected_components",
        "svd_plus_plus",
        "triangle_count",
        "triplets",
    )
    VERTICES_FIELD_NUMBER: _ClassVar[int]
    EDGES_FIELD_NUMBER: _ClassVar[int]
    AGGREGATE_MESSAGES_FIELD_NUMBER: _ClassVar[int]
    BFS_FIELD_NUMBER: _ClassVar[int]
    CONNECTED_COMPONENTS_FIELD_NUMBER: _ClassVar[int]
    DEGREES_FIELD_NUMBER: _ClassVar[int]
    DROP_ISOLATED_VERTICES_FIELD_NUMBER: _ClassVar[int]
    FILTER_EDGES_FIELD_NUMBER: _ClassVar[int]
    FILTER_VERTICES_FIELD_NUMBER: _ClassVar[int]
    FIND_FIELD_NUMBER: _ClassVar[int]
    IN_DEGREES_FIELD_NUMBER: _ClassVar[int]
    LABEL_PROPAGATION_FIELD_NUMBER: _ClassVar[int]
    OUT_DEGREES_FIELD_NUMBER: _ClassVar[int]
    PAGE_RANK_FIELD_NUMBER: _ClassVar[int]
    PARALLEL_PERSONALIZED_PAGE_RANK_FIELD_NUMBER: _ClassVar[int]
    PREGEL_FIELD_NUMBER: _ClassVar[int]
    SHORTEST_PATHS_FIELD_NUMBER: _ClassVar[int]
    STRONGLY_CONNECTED_COMPONENTS_FIELD_NUMBER: _ClassVar[int]
    SVD_PLUS_PLUS_FIELD_NUMBER: _ClassVar[int]
    TRIANGLE_COUNT_FIELD_NUMBER: _ClassVar[int]
    TRIPLETS_FIELD_NUMBER: _ClassVar[int]
    vertices: bytes
    edges: bytes
    aggregate_messages: AggregateMessages
    bfs: BFS
    connected_components: ConnectedComponents
    degrees: Degrees
    drop_isolated_vertices: DropIsolatedVertices
    filter_edges: FilterEdges
    filter_vertices: FilterVertices
    find: Find
    in_degrees: InDegrees
    label_propagation: LabelPropagation
    out_degrees: OutDegrees
    page_rank: PageRank
    parallel_personalized_page_rank: ParallelPersonalizedPageRank
    pregel: Pregel
    shortest_paths: ShortestPaths
    strongly_connected_components: StronglyConnectedComponents
    svd_plus_plus: SVDPlusPlus
    triangle_count: TriangleCount
    triplets: Triplets
    def __init__(
        self,
        vertices: _Optional[bytes] = ...,
        edges: _Optional[bytes] = ...,
        aggregate_messages: _Optional[_Union[AggregateMessages, _Mapping]] = ...,
        bfs: _Optional[_Union[BFS, _Mapping]] = ...,
        connected_components: _Optional[_Union[ConnectedComponents, _Mapping]] = ...,
        degrees: _Optional[_Union[Degrees, _Mapping]] = ...,
        drop_isolated_vertices: _Optional[_Union[DropIsolatedVertices, _Mapping]] = ...,
        filter_edges: _Optional[_Union[FilterEdges, _Mapping]] = ...,
        filter_vertices: _Optional[_Union[FilterVertices, _Mapping]] = ...,
        find: _Optional[_Union[Find, _Mapping]] = ...,
        in_degrees: _Optional[_Union[InDegrees, _Mapping]] = ...,
        label_propagation: _Optional[_Union[LabelPropagation, _Mapping]] = ...,
        out_degrees: _Optional[_Union[OutDegrees, _Mapping]] = ...,
        page_rank: _Optional[_Union[PageRank, _Mapping]] = ...,
        parallel_personalized_page_rank: _Optional[
            _Union[ParallelPersonalizedPageRank, _Mapping]
        ] = ...,
        pregel: _Optional[_Union[Pregel, _Mapping]] = ...,
        shortest_paths: _Optional[_Union[ShortestPaths, _Mapping]] = ...,
        strongly_connected_components: _Optional[
            _Union[StronglyConnectedComponents, _Mapping]
        ] = ...,
        svd_plus_plus: _Optional[_Union[SVDPlusPlus, _Mapping]] = ...,
        triangle_count: _Optional[_Union[TriangleCount, _Mapping]] = ...,
        triplets: _Optional[_Union[Triplets, _Mapping]] = ...,
    ) -> None: ...

class ColumnOrExpression(_message.Message):
    __slots__ = ("col", "expr")
    COL_FIELD_NUMBER: _ClassVar[int]
    EXPR_FIELD_NUMBER: _ClassVar[int]
    col: bytes
    expr: str
    def __init__(self, col: _Optional[bytes] = ..., expr: _Optional[str] = ...) -> None: ...

class StringOrLongID(_message.Message):
    __slots__ = ("long_id", "string_id")
    LONG_ID_FIELD_NUMBER: _ClassVar[int]
    STRING_ID_FIELD_NUMBER: _ClassVar[int]
    long_id: int
    string_id: str
    def __init__(self, long_id: _Optional[int] = ..., string_id: _Optional[str] = ...) -> None: ...

class AggregateMessages(_message.Message):
    __slots__ = ("agg_col", "send_to_src", "send_to_dst")
    AGG_COL_FIELD_NUMBER: _ClassVar[int]
    SEND_TO_SRC_FIELD_NUMBER: _ClassVar[int]
    SEND_TO_DST_FIELD_NUMBER: _ClassVar[int]
    agg_col: ColumnOrExpression
    send_to_src: ColumnOrExpression
    send_to_dst: ColumnOrExpression
    def __init__(
        self,
        agg_col: _Optional[_Union[ColumnOrExpression, _Mapping]] = ...,
        send_to_src: _Optional[_Union[ColumnOrExpression, _Mapping]] = ...,
        send_to_dst: _Optional[_Union[ColumnOrExpression, _Mapping]] = ...,
    ) -> None: ...

class BFS(_message.Message):
    __slots__ = ("from_expr", "to_expr", "edge_filter", "max_path_length")
    FROM_EXPR_FIELD_NUMBER: _ClassVar[int]
    TO_EXPR_FIELD_NUMBER: _ClassVar[int]
    EDGE_FILTER_FIELD_NUMBER: _ClassVar[int]
    MAX_PATH_LENGTH_FIELD_NUMBER: _ClassVar[int]
    from_expr: ColumnOrExpression
    to_expr: ColumnOrExpression
    edge_filter: ColumnOrExpression
    max_path_length: int
    def __init__(
        self,
        from_expr: _Optional[_Union[ColumnOrExpression, _Mapping]] = ...,
        to_expr: _Optional[_Union[ColumnOrExpression, _Mapping]] = ...,
        edge_filter: _Optional[_Union[ColumnOrExpression, _Mapping]] = ...,
        max_path_length: _Optional[int] = ...,
    ) -> None: ...

class ConnectedComponents(_message.Message):
    __slots__ = ("algorithm", "checkpoint_interval", "broadcast_threshold")
    ALGORITHM_FIELD_NUMBER: _ClassVar[int]
    CHECKPOINT_INTERVAL_FIELD_NUMBER: _ClassVar[int]
    BROADCAST_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    algorithm: str
    checkpoint_interval: int
    broadcast_threshold: int
    def __init__(
        self,
        algorithm: _Optional[str] = ...,
        checkpoint_interval: _Optional[int] = ...,
        broadcast_threshold: _Optional[int] = ...,
    ) -> None: ...

class Degrees(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class DropIsolatedVertices(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class FilterEdges(_message.Message):
    __slots__ = ("condition",)
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    condition: ColumnOrExpression
    def __init__(
        self, condition: _Optional[_Union[ColumnOrExpression, _Mapping]] = ...
    ) -> None: ...

class FilterVertices(_message.Message):
    __slots__ = ("condition",)
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    condition: ColumnOrExpression
    def __init__(
        self, condition: _Optional[_Union[ColumnOrExpression, _Mapping]] = ...
    ) -> None: ...

class Find(_message.Message):
    __slots__ = ("pattern",)
    PATTERN_FIELD_NUMBER: _ClassVar[int]
    pattern: str
    def __init__(self, pattern: _Optional[str] = ...) -> None: ...

class InDegrees(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class LabelPropagation(_message.Message):
    __slots__ = ("max_iter",)
    MAX_ITER_FIELD_NUMBER: _ClassVar[int]
    max_iter: int
    def __init__(self, max_iter: _Optional[int] = ...) -> None: ...

class OutDegrees(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class PageRank(_message.Message):
    __slots__ = ("reset_probability", "source_id", "max_iter", "tol")
    RESET_PROBABILITY_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    MAX_ITER_FIELD_NUMBER: _ClassVar[int]
    TOL_FIELD_NUMBER: _ClassVar[int]
    reset_probability: float
    source_id: StringOrLongID
    max_iter: int
    tol: float
    def __init__(
        self,
        reset_probability: _Optional[float] = ...,
        source_id: _Optional[_Union[StringOrLongID, _Mapping]] = ...,
        max_iter: _Optional[int] = ...,
        tol: _Optional[float] = ...,
    ) -> None: ...

class ParallelPersonalizedPageRank(_message.Message):
    __slots__ = ("reset_probability", "source_ids", "max_iter")
    RESET_PROBABILITY_FIELD_NUMBER: _ClassVar[int]
    SOURCE_IDS_FIELD_NUMBER: _ClassVar[int]
    MAX_ITER_FIELD_NUMBER: _ClassVar[int]
    reset_probability: float
    source_ids: _containers.RepeatedCompositeFieldContainer[StringOrLongID]
    max_iter: int
    def __init__(
        self,
        reset_probability: _Optional[float] = ...,
        source_ids: _Optional[_Iterable[_Union[StringOrLongID, _Mapping]]] = ...,
        max_iter: _Optional[int] = ...,
    ) -> None: ...

class Pregel(_message.Message):
    __slots__ = (
        "agg_msgs",
        "send_msg_to_dst",
        "send_msg_to_src",
        "checkpoint_interval",
        "max_iter",
        "additional_col_name",
        "additional_col_initial",
        "additional_col_upd",
    )
    AGG_MSGS_FIELD_NUMBER: _ClassVar[int]
    SEND_MSG_TO_DST_FIELD_NUMBER: _ClassVar[int]
    SEND_MSG_TO_SRC_FIELD_NUMBER: _ClassVar[int]
    CHECKPOINT_INTERVAL_FIELD_NUMBER: _ClassVar[int]
    MAX_ITER_FIELD_NUMBER: _ClassVar[int]
    ADDITIONAL_COL_NAME_FIELD_NUMBER: _ClassVar[int]
    ADDITIONAL_COL_INITIAL_FIELD_NUMBER: _ClassVar[int]
    ADDITIONAL_COL_UPD_FIELD_NUMBER: _ClassVar[int]
    agg_msgs: ColumnOrExpression
    send_msg_to_dst: _containers.RepeatedCompositeFieldContainer[ColumnOrExpression]
    send_msg_to_src: _containers.RepeatedCompositeFieldContainer[ColumnOrExpression]
    checkpoint_interval: int
    max_iter: int
    additional_col_name: str
    additional_col_initial: ColumnOrExpression
    additional_col_upd: ColumnOrExpression
    def __init__(
        self,
        agg_msgs: _Optional[_Union[ColumnOrExpression, _Mapping]] = ...,
        send_msg_to_dst: _Optional[_Iterable[_Union[ColumnOrExpression, _Mapping]]] = ...,
        send_msg_to_src: _Optional[_Iterable[_Union[ColumnOrExpression, _Mapping]]] = ...,
        checkpoint_interval: _Optional[int] = ...,
        max_iter: _Optional[int] = ...,
        additional_col_name: _Optional[str] = ...,
        additional_col_initial: _Optional[_Union[ColumnOrExpression, _Mapping]] = ...,
        additional_col_upd: _Optional[_Union[ColumnOrExpression, _Mapping]] = ...,
    ) -> None: ...

class ShortestPaths(_message.Message):
    __slots__ = ("landmarks",)
    LANDMARKS_FIELD_NUMBER: _ClassVar[int]
    landmarks: _containers.RepeatedCompositeFieldContainer[StringOrLongID]
    def __init__(
        self, landmarks: _Optional[_Iterable[_Union[StringOrLongID, _Mapping]]] = ...
    ) -> None: ...

class StronglyConnectedComponents(_message.Message):
    __slots__ = ("max_iter",)
    MAX_ITER_FIELD_NUMBER: _ClassVar[int]
    max_iter: int
    def __init__(self, max_iter: _Optional[int] = ...) -> None: ...

class SVDPlusPlus(_message.Message):
    __slots__ = (
        "rank",
        "max_iter",
        "min_value",
        "max_value",
        "gamma1",
        "gamma2",
        "gamma6",
        "gamma7",
    )
    RANK_FIELD_NUMBER: _ClassVar[int]
    MAX_ITER_FIELD_NUMBER: _ClassVar[int]
    MIN_VALUE_FIELD_NUMBER: _ClassVar[int]
    MAX_VALUE_FIELD_NUMBER: _ClassVar[int]
    GAMMA1_FIELD_NUMBER: _ClassVar[int]
    GAMMA2_FIELD_NUMBER: _ClassVar[int]
    GAMMA6_FIELD_NUMBER: _ClassVar[int]
    GAMMA7_FIELD_NUMBER: _ClassVar[int]
    rank: int
    max_iter: int
    min_value: float
    max_value: float
    gamma1: float
    gamma2: float
    gamma6: float
    gamma7: float
    def __init__(
        self,
        rank: _Optional[int] = ...,
        max_iter: _Optional[int] = ...,
        min_value: _Optional[float] = ...,
        max_value: _Optional[float] = ...,
        gamma1: _Optional[float] = ...,
        gamma2: _Optional[float] = ...,
        gamma6: _Optional[float] = ...,
        gamma7: _Optional[float] = ...,
    ) -> None: ...

class TriangleCount(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class Triplets(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
