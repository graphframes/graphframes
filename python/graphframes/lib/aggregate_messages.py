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

from typing import Any

from pyspark import SparkContext
from pyspark.sql import DataFrame, functions as sqlfunctions, SparkSession, Column


def _java_api(jsc: SparkContext) -> Any:
    javaClassName = "org.graphframes.GraphFramePythonAPI"
    return jsc._jvm.Thread.currentThread().getContextClassLoader().loadClass(javaClassName) \
            .newInstance()


class _ClassProperty:
    """Custom read-only class property descriptor.

    The underlying method should take the class as the sole argument.
    """

    def __init__(self, f: callable) -> None:
        self.f = f
        self.__doc__ = f.__doc__

    def __get__(self, instance: Any, owner: type) -> Any:
        return self.f(owner)


class AggregateMessages:
    """Collection of utilities usable with :meth:`graphframes.GraphFrame.aggregateMessages()`."""

    @_ClassProperty
    def src(cls) -> Column:
        """Reference for source column, used for specifying messages."""
        jvm_gf_api = _java_api(SparkContext)
        return sqlfunctions.col(jvm_gf_api.SRC())

    @_ClassProperty
    def dst(cls) -> Column:
        """Reference for destination column, used for specifying messages."""
        jvm_gf_api = _java_api(SparkContext)
        return sqlfunctions.col(jvm_gf_api.DST())

    @_ClassProperty
    def edge(cls) -> Column:
        """Reference for edge column, used for specifying messages."""
        jvm_gf_api = _java_api(SparkContext)
        return sqlfunctions.col(jvm_gf_api.EDGE())

    @_ClassProperty
    def msg(cls) -> Column:
        """Reference for message column, used for specifying aggregation function."""
        jvm_gf_api = _java_api(SparkContext)
        return sqlfunctions.col(jvm_gf_api.aggregateMessages().MSG_COL_NAME())

    @staticmethod
    def getCachedDataFrame(df: DataFrame) -> DataFrame:
        """
        Create a new cached copy of a DataFrame.

        This utility method is useful for iterative DataFrame-based algorithms. See Scala
        documentation for more details.

        WARNING: This is NOT the same as `DataFrame.cache()`.
                 The original DataFrame will NOT be cached.
        """
        spark = SparkSession.getActiveSession()
        jvm_gf_api = _java_api(spark._sc)
        jdf = jvm_gf_api.aggregateMessages().getCachedDataFrame(df._jdf)
        return DataFrame(jdf, spark)
