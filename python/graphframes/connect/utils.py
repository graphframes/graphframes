from pyspark.sql.connect.client import SparkConnectClient
from pyspark.sql.connect.column import Column
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.expressions import Expression
from pyspark.sql.connect.plan import LogicalPlan

from .proto.graphframes_pb2 import ColumnOrExpression


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
