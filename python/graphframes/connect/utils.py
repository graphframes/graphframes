from __future__ import annotations

from pyspark.sql.connect.client import SparkConnectClient
from pyspark.sql.connect.column import Column
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.expressions import Expression
from pyspark.sql.connect.plan import LogicalPlan
from pyspark.storagelevel import StorageLevel

from .proto.graphframes_pb2 import (
    ColumnOrExpression,
)
from .proto.graphframes_pb2 import StorageLevel as StorageLevelProto
from .proto.graphframes_pb2 import (
    StringOrLongID,
)


def dataframe_to_proto(df: DataFrame, client: SparkConnectClient) -> bytes:
    plan = df._plan
    assert plan is not None
    assert isinstance(plan, LogicalPlan)
    return plan.to_proto(client).SerializeToString()


def column_to_proto(col: Column, client: SparkConnectClient) -> bytes:
    expr = col._expr
    assert expr is not None
    assert isinstance(expr, Expression)
    return expr.to_plan(client).SerializeToString()


def make_column_or_expr(col: Column | str, client: SparkConnectClient) -> ColumnOrExpression:
    if isinstance(col, Column):
        return ColumnOrExpression(col=column_to_proto(col, client))
    else:
        return ColumnOrExpression(expr=col)


def make_str_or_long_id(str_or_long: str | int) -> StringOrLongID:
    if isinstance(str_or_long, str):
        return StringOrLongID(string_id=str_or_long)
    else:
        return StringOrLongID(long_id=str_or_long)


def storage_level_to_proto(storage_level: StorageLevel) -> StorageLevelProto:
    if storage_level == StorageLevel.DISK_ONLY:
        return StorageLevelProto(disk_only=True)
    elif storage_level == StorageLevel.DISK_ONLY_2:
        return StorageLevelProto(disk_only_2=True)
    elif storage_level == StorageLevel.DISK_ONLY_3:
        return StorageLevelProto(disk_only_3=True)
    elif storage_level == StorageLevel.MEMORY_AND_DISK:
        return StorageLevelProto(memory_and_disk=True)
    elif storage_level == StorageLevel.MEMORY_AND_DISK_2:
        return StorageLevelProto(memory_and_disk_2=True)
    elif storage_level == StorageLevel.MEMORY_ONLY:
        return StorageLevelProto(memory_only=True)
    elif storage_level == StorageLevel.MEMORY_ONLY_2:
        return StorageLevelProto(memory_only_2=True)
    elif storage_level == StorageLevel.MEMORY_AND_DISK_DESER:
        return StorageLevelProto(memory_and_disk_deser=True)
    else:
        raise ValueError(f"Unknown storage level: {storage_level}")
