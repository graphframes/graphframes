from collections.abc import Iterable as _Iterable
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar
from typing import Optional as _Optional
from typing import Union as _Union

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf.internal import containers as _containers

DESCRIPTOR: _descriptor.FileDescriptor

class GraphFramesAPI(_message.Message):
    __slots__ = ()
    VERTICES_FIELD_NUMBER: _ClassVar[int]
    EDGES_FIELD_NUMBER: _ClassVar[int]
    AGGREGATE_MESSAGES_FIELD_NUMBER: _ClassVar[int]
    BFS_FIELD_NUMBER: _ClassVar[int]
    CONNECTED_COMPONENTS_FIELD_NUMBER: _ClassVar[int]
    DROP_ISOLATED_VERTICES_FIELD_NUMBER: _ClassVar[int]
    DETECTING_CYCLES_FIELD_NUMBER: _ClassVar[int]
    FILTER_EDGES_FIELD_NUMBER: _ClassVar[int]
    FILTER_VERTICES_FIELD_NUMBER: _ClassVar[int]
    FIND_FIELD_NUMBER: _ClassVar[int]
    LABEL_PROPAGATION_FIELD_NUMBER: _ClassVar[int]
    PAGE_RANK_FIELD_NUMBER: _ClassVar[int]
    PARALLEL_PERSONALIZED_PAGE_RANK_FIELD_NUMBER: _ClassVar[int]
    POWER_ITERATION_CLUSTERING_FIELD_NUMBER: _ClassVar[int]
    PREGEL_FIELD_NUMBER: _ClassVar[int]
    SHORTEST_PATHS_FIELD_NUMBER: _ClassVar[int]
    STRONGLY_CONNECTED_COMPONENTS_FIELD_NUMBER: _ClassVar[int]
    SVD_PLUS_PLUS_FIELD_NUMBER: _ClassVar[int]
    TRIANGLE_COUNT_FIELD_NUMBER: _ClassVar[int]
    TRIPLETS_FIELD_NUMBER: _ClassVar[int]
    MIS_FIELD_NUMBER: _ClassVar[int]
    vertices: bytes
    edges: bytes
    aggregate_messages: AggregateMessages
    bfs: BFS
    connected_components: ConnectedComponents
    drop_isolated_vertices: DropIsolatedVertices
    detecting_cycles: DetectingCycles
    filter_edges: FilterEdges
    filter_vertices: FilterVertices
    find: Find
    label_propagation: LabelPropagation
    page_rank: PageRank
    parallel_personalized_page_rank: ParallelPersonalizedPageRank
    power_iteration_clustering: PowerIterationClustering
    pregel: Pregel
    shortest_paths: ShortestPaths
    strongly_connected_components: StronglyConnectedComponents
    svd_plus_plus: SVDPlusPlus
    triangle_count: TriangleCount
    triplets: Triplets
    mis: MaximalIndependentSet
    def __init__(
        self,
        vertices: _Optional[bytes] = ...,
        edges: _Optional[bytes] = ...,
        aggregate_messages: _Optional[_Union[AggregateMessages, _Mapping]] = ...,
        bfs: _Optional[_Union[BFS, _Mapping]] = ...,
        connected_components: _Optional[_Union[ConnectedComponents, _Mapping]] = ...,
        drop_isolated_vertices: _Optional[_Union[DropIsolatedVertices, _Mapping]] = ...,
        detecting_cycles: _Optional[_Union[DetectingCycles, _Mapping]] = ...,
        filter_edges: _Optional[_Union[FilterEdges, _Mapping]] = ...,
        filter_vertices: _Optional[_Union[FilterVertices, _Mapping]] = ...,
        find: _Optional[_Union[Find, _Mapping]] = ...,
        label_propagation: _Optional[_Union[LabelPropagation, _Mapping]] = ...,
        page_rank: _Optional[_Union[PageRank, _Mapping]] = ...,
        parallel_personalized_page_rank: _Optional[
            _Union[ParallelPersonalizedPageRank, _Mapping]
        ] = ...,
        power_iteration_clustering: _Optional[_Union[PowerIterationClustering, _Mapping]] = ...,
        pregel: _Optional[_Union[Pregel, _Mapping]] = ...,
        shortest_paths: _Optional[_Union[ShortestPaths, _Mapping]] = ...,
        strongly_connected_components: _Optional[
            _Union[StronglyConnectedComponents, _Mapping]
        ] = ...,
        svd_plus_plus: _Optional[_Union[SVDPlusPlus, _Mapping]] = ...,
        triangle_count: _Optional[_Union[TriangleCount, _Mapping]] = ...,
        triplets: _Optional[_Union[Triplets, _Mapping]] = ...,
        mis: _Optional[_Union[MaximalIndependentSet, _Mapping]] = ...,
    ) -> None: ...

class StorageLevel(_message.Message):
    __slots__ = ()
    DISK_ONLY_FIELD_NUMBER: _ClassVar[int]
    DISK_ONLY_2_FIELD_NUMBER: _ClassVar[int]
    DISK_ONLY_3_FIELD_NUMBER: _ClassVar[int]
    MEMORY_AND_DISK_FIELD_NUMBER: _ClassVar[int]
    MEMORY_AND_DISK_2_FIELD_NUMBER: _ClassVar[int]
    MEMORY_AND_DISK_DESER_FIELD_NUMBER: _ClassVar[int]
    MEMORY_ONLY_FIELD_NUMBER: _ClassVar[int]
    MEMORY_ONLY_2_FIELD_NUMBER: _ClassVar[int]
    disk_only: bool
    disk_only_2: bool
    disk_only_3: bool
    memory_and_disk: bool
    memory_and_disk_2: bool
    memory_and_disk_deser: bool
    memory_only: bool
    memory_only_2: bool
    def __init__(
        self,
        disk_only: _Optional[bool] = ...,
        disk_only_2: _Optional[bool] = ...,
        disk_only_3: _Optional[bool] = ...,
        memory_and_disk: _Optional[bool] = ...,
        memory_and_disk_2: _Optional[bool] = ...,
        memory_and_disk_deser: _Optional[bool] = ...,
        memory_only: _Optional[bool] = ...,
        memory_only_2: _Optional[bool] = ...,
    ) -> None: ...

class ColumnOrExpression(_message.Message):
    __slots__ = ()
    COL_FIELD_NUMBER: _ClassVar[int]
    EXPR_FIELD_NUMBER: _ClassVar[int]
    col: bytes
    expr: str
    def __init__(self, col: _Optional[bytes] = ..., expr: _Optional[str] = ...) -> None: ...

class StringOrLongID(_message.Message):
    __slots__ = ()
    LONG_ID_FIELD_NUMBER: _ClassVar[int]
    STRING_ID_FIELD_NUMBER: _ClassVar[int]
    long_id: int
    string_id: str
    def __init__(self, long_id: _Optional[int] = ..., string_id: _Optional[str] = ...) -> None: ...

class AggregateMessages(_message.Message):
    __slots__ = ()
    AGG_COL_FIELD_NUMBER: _ClassVar[int]
    SEND_TO_SRC_FIELD_NUMBER: _ClassVar[int]
    SEND_TO_DST_FIELD_NUMBER: _ClassVar[int]
    STORAGE_LEVEL_FIELD_NUMBER: _ClassVar[int]
    agg_col: _containers.RepeatedCompositeFieldContainer[ColumnOrExpression]
    send_to_src: _containers.RepeatedCompositeFieldContainer[ColumnOrExpression]
    send_to_dst: _containers.RepeatedCompositeFieldContainer[ColumnOrExpression]
    storage_level: StorageLevel
    def __init__(
        self,
        agg_col: _Optional[_Iterable[_Union[ColumnOrExpression, _Mapping]]] = ...,
        send_to_src: _Optional[_Iterable[_Union[ColumnOrExpression, _Mapping]]] = ...,
        send_to_dst: _Optional[_Iterable[_Union[ColumnOrExpression, _Mapping]]] = ...,
        storage_level: _Optional[_Union[StorageLevel, _Mapping]] = ...,
    ) -> None: ...

class BFS(_message.Message):
    __slots__ = ()
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
    __slots__ = ()
    ALGORITHM_FIELD_NUMBER: _ClassVar[int]
    CHECKPOINT_INTERVAL_FIELD_NUMBER: _ClassVar[int]
    BROADCAST_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    USE_LABELS_AS_COMPONENTS_FIELD_NUMBER: _ClassVar[int]
    USE_LOCAL_CHECKPOINTS_FIELD_NUMBER: _ClassVar[int]
    MAX_ITER_FIELD_NUMBER: _ClassVar[int]
    STORAGE_LEVEL_FIELD_NUMBER: _ClassVar[int]
    algorithm: str
    checkpoint_interval: int
    broadcast_threshold: int
    use_labels_as_components: bool
    use_local_checkpoints: bool
    max_iter: int
    storage_level: StorageLevel
    def __init__(
        self,
        algorithm: _Optional[str] = ...,
        checkpoint_interval: _Optional[int] = ...,
        broadcast_threshold: _Optional[int] = ...,
        use_labels_as_components: _Optional[bool] = ...,
        use_local_checkpoints: _Optional[bool] = ...,
        max_iter: _Optional[int] = ...,
        storage_level: _Optional[_Union[StorageLevel, _Mapping]] = ...,
    ) -> None: ...

class DetectingCycles(_message.Message):
    __slots__ = ()
    USE_LOCAL_CHECKPOINTS_FIELD_NUMBER: _ClassVar[int]
    CHECKPOINT_INTERVAL_FIELD_NUMBER: _ClassVar[int]
    STORAGE_LEVEL_FIELD_NUMBER: _ClassVar[int]
    use_local_checkpoints: bool
    checkpoint_interval: int
    storage_level: StorageLevel
    def __init__(
        self,
        use_local_checkpoints: _Optional[bool] = ...,
        checkpoint_interval: _Optional[int] = ...,
        storage_level: _Optional[_Union[StorageLevel, _Mapping]] = ...,
    ) -> None: ...

class DropIsolatedVertices(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class FilterEdges(_message.Message):
    __slots__ = ()
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    condition: ColumnOrExpression
    def __init__(
        self, condition: _Optional[_Union[ColumnOrExpression, _Mapping]] = ...
    ) -> None: ...

class FilterVertices(_message.Message):
    __slots__ = ()
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    condition: ColumnOrExpression
    def __init__(
        self, condition: _Optional[_Union[ColumnOrExpression, _Mapping]] = ...
    ) -> None: ...

class Find(_message.Message):
    __slots__ = ()
    PATTERN_FIELD_NUMBER: _ClassVar[int]
    pattern: str
    def __init__(self, pattern: _Optional[str] = ...) -> None: ...

class LabelPropagation(_message.Message):
    __slots__ = ()
    ALGORITHM_FIELD_NUMBER: _ClassVar[int]
    MAX_ITER_FIELD_NUMBER: _ClassVar[int]
    USE_LOCAL_CHECKPOINTS_FIELD_NUMBER: _ClassVar[int]
    CHECKPOINT_INTERVAL_FIELD_NUMBER: _ClassVar[int]
    STORAGE_LEVEL_FIELD_NUMBER: _ClassVar[int]
    algorithm: str
    max_iter: int
    use_local_checkpoints: bool
    checkpoint_interval: int
    storage_level: StorageLevel
    def __init__(
        self,
        algorithm: _Optional[str] = ...,
        max_iter: _Optional[int] = ...,
        use_local_checkpoints: _Optional[bool] = ...,
        checkpoint_interval: _Optional[int] = ...,
        storage_level: _Optional[_Union[StorageLevel, _Mapping]] = ...,
    ) -> None: ...

class PageRank(_message.Message):
    __slots__ = ()
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
    __slots__ = ()
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

class PowerIterationClustering(_message.Message):
    __slots__ = ()
    K_FIELD_NUMBER: _ClassVar[int]
    MAX_ITER_FIELD_NUMBER: _ClassVar[int]
    WEIGHT_COL_FIELD_NUMBER: _ClassVar[int]
    k: int
    max_iter: int
    weight_col: str
    def __init__(
        self,
        k: _Optional[int] = ...,
        max_iter: _Optional[int] = ...,
        weight_col: _Optional[str] = ...,
    ) -> None: ...

class Pregel(_message.Message):
    __slots__ = ()
    AGG_MSGS_FIELD_NUMBER: _ClassVar[int]
    SEND_MSG_TO_DST_FIELD_NUMBER: _ClassVar[int]
    SEND_MSG_TO_SRC_FIELD_NUMBER: _ClassVar[int]
    CHECKPOINT_INTERVAL_FIELD_NUMBER: _ClassVar[int]
    MAX_ITER_FIELD_NUMBER: _ClassVar[int]
    ADDITIONAL_COL_NAME_FIELD_NUMBER: _ClassVar[int]
    ADDITIONAL_COL_INITIAL_FIELD_NUMBER: _ClassVar[int]
    ADDITIONAL_COL_UPD_FIELD_NUMBER: _ClassVar[int]
    EARLY_STOPPING_FIELD_NUMBER: _ClassVar[int]
    USE_LOCAL_CHECKPOINTS_FIELD_NUMBER: _ClassVar[int]
    STORAGE_LEVEL_FIELD_NUMBER: _ClassVar[int]
    STOP_IF_ALL_NON_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    INITIAL_ACTIVE_EXPR_FIELD_NUMBER: _ClassVar[int]
    UPDATE_ACTIVE_EXPR_FIELD_NUMBER: _ClassVar[int]
    SKIP_MESSAGES_FROM_NON_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    agg_msgs: ColumnOrExpression
    send_msg_to_dst: _containers.RepeatedCompositeFieldContainer[ColumnOrExpression]
    send_msg_to_src: _containers.RepeatedCompositeFieldContainer[ColumnOrExpression]
    checkpoint_interval: int
    max_iter: int
    additional_col_name: str
    additional_col_initial: ColumnOrExpression
    additional_col_upd: ColumnOrExpression
    early_stopping: bool
    use_local_checkpoints: bool
    storage_level: StorageLevel
    stop_if_all_non_active: bool
    initial_active_expr: ColumnOrExpression
    update_active_expr: ColumnOrExpression
    skip_messages_from_non_active: bool
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
        early_stopping: _Optional[bool] = ...,
        use_local_checkpoints: _Optional[bool] = ...,
        storage_level: _Optional[_Union[StorageLevel, _Mapping]] = ...,
        stop_if_all_non_active: _Optional[bool] = ...,
        initial_active_expr: _Optional[_Union[ColumnOrExpression, _Mapping]] = ...,
        update_active_expr: _Optional[_Union[ColumnOrExpression, _Mapping]] = ...,
        skip_messages_from_non_active: _Optional[bool] = ...,
    ) -> None: ...

class ShortestPaths(_message.Message):
    __slots__ = ()
    LANDMARKS_FIELD_NUMBER: _ClassVar[int]
    ALGORITHM_FIELD_NUMBER: _ClassVar[int]
    USE_LOCAL_CHECKPOINTS_FIELD_NUMBER: _ClassVar[int]
    CHECKPOINT_INTERVAL_FIELD_NUMBER: _ClassVar[int]
    STORAGE_LEVEL_FIELD_NUMBER: _ClassVar[int]
    landmarks: _containers.RepeatedCompositeFieldContainer[StringOrLongID]
    algorithm: str
    use_local_checkpoints: bool
    checkpoint_interval: int
    storage_level: StorageLevel
    def __init__(
        self,
        landmarks: _Optional[_Iterable[_Union[StringOrLongID, _Mapping]]] = ...,
        algorithm: _Optional[str] = ...,
        use_local_checkpoints: _Optional[bool] = ...,
        checkpoint_interval: _Optional[int] = ...,
        storage_level: _Optional[_Union[StorageLevel, _Mapping]] = ...,
    ) -> None: ...

class StronglyConnectedComponents(_message.Message):
    __slots__ = ()
    MAX_ITER_FIELD_NUMBER: _ClassVar[int]
    max_iter: int
    def __init__(self, max_iter: _Optional[int] = ...) -> None: ...

class SVDPlusPlus(_message.Message):
    __slots__ = ()
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
    STORAGE_LEVEL_FIELD_NUMBER: _ClassVar[int]
    storage_level: StorageLevel
    def __init__(self, storage_level: _Optional[_Union[StorageLevel, _Mapping]] = ...) -> None: ...

class Triplets(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class MaximalIndependentSet(_message.Message):
    __slots__ = ()
    CHECKPOINT_INTERVAL_FIELD_NUMBER: _ClassVar[int]
    STORAGE_LEVEL_FIELD_NUMBER: _ClassVar[int]
    USE_LOCAL_CHECKPOINTS_FIELD_NUMBER: _ClassVar[int]
    SEED_FIELD_NUMBER: _ClassVar[int]
    checkpoint_interval: int
    storage_level: StorageLevel
    use_local_checkpoints: bool
    seed: int
    def __init__(
        self,
        checkpoint_interval: _Optional[int] = ...,
        storage_level: _Optional[_Union[StorageLevel, _Mapping]] = ...,
        use_local_checkpoints: _Optional[bool] = ...,
        seed: _Optional[int] = ...,
    ) -> None: ...
