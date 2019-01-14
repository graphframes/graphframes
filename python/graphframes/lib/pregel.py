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

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.ml.wrapper import JavaWrapper, _jvm


class Pregel(JavaWrapper):
    r"""
    Implements a Pregel-like bulk-synchronous message-passing API based on DataFrame operations.

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

    >>> from graphframes import GraphFrame
    >>> from pyspark.sql.functions import coalesce, col, lit, sum, when
    >>> from graphframes.lib import Pregel
    >>> edges = spark.createDataFrame([[0, 1],
    ...                                [1, 2],
    ...                                [2, 4],
    ...                                [2, 0],
    ...                                [3, 4], # 3 has no in-links
    ...                                [4, 0],
    ...                                [4, 2]], ["src", "dst"])
    >>> edges.cache()
    >>> vertices = spark.createDataFrame([[0], [1], [2], [3], [4]], ["id"])
    >>> numVertices = vertices.count()
    >>> vertices = GraphFrame(vertices, edges).outDegrees
    >>> vertices.cache()
    >>> graph = GraphFrame(vertices, edges)
    >>> alpha = 0.15
    >>> ranks = graph.pregel \
    ...     .setMaxIter(5) \
    ...     .withVertexColumn("rank", lit(1.0 / numVertices), \
    ...         coalesce(Pregel.msg(), lit(0.0)) * lit(1.0 - alpha) + lit(alpha / numVertices)) \
    ...     .sendMsgToDst(Pregel.src("rank") / Pregel.src("outDegree")) \
    ...     .aggMsgs(sum(Pregel.msg())) \
    ...     .run()
    """

    def __init__(self, graph):
        super(Pregel, self).__init__()
        self.graph = graph
        self._java_obj = self._new_java_obj("org.graphframes.lib.Pregel", graph._jvm_graph)

    def setMaxIter(self, value):
        """
        Sets the max number of iterations (default: 10).
        """
        self._java_obj.setMaxIter(int(value))
        return self

    def setCheckpointInterval(self, value):
        """
        Sets the number of iterations between two checkpoints (default: 2).

        This is an advanced control to balance query plan optimization and checkpoint data I/O cost.
        In most cases, you should keep the default value.

        Checkpoint is disabled if this is set to 0.
        """
        self._java_obj.setCheckpointInterval(int(value))
        return self

    def withVertexColumn(self, colName, initialExpr, updateAfterAggMsgsExpr):
        """
        Defines an additional vertex column at the start of run and how to update it in each iteration.

        You can call it multiple times to add more than one additional vertex columns.

        :param colName: the name of the additional vertex column.
                        It cannot be an existing vertex column in the graph.
        :param initialExpr: the expression to initialize the additional vertex column.
                            You can reference all original vertex columns in this expression.
        :param updateAfterAggMsgsExpr: the expression to update the additional vertex column after messages aggregation.
                                       You can reference all original vertex columns, additional vertex columns, and the
                                       aggregated message column using :func:`msg`.
                                       If the vertex received no messages, the message column would be null.
        """
        self._java_obj.withVertexColumn(colName, initialExpr._jc, updateAfterAggMsgsExpr._jc)
        return self

    def sendMsgToSrc(self, msgExpr):
        """
        Defines a message to send to the source vertex of each edge triplet.

        You can call it multiple times to send more than one messages.

        See method :func:`sendMsgToDst`.

        :param msgExpr: the expression of the message to send to the source vertex given a (src, edge, dst) triplet.
                        Source/destination vertex properties and edge properties are nested under columns `src`, `dst`,
                        and `edge`, respectively.
                        You can reference them using :func:`src`, :func:`dst`, and :func:`edge`.
                        Null messages are not included in message aggregation.
        """
        self._java_obj.sendMsgToSrc(msgExpr._jc)
        return self

    def sendMsgToDst(self, msgExpr):
        """
        Defines a message to send to the destination vertex of each edge triplet.

        You can call it multiple times to send more than one messages.

        See method :func:`sendMsgToSrc`.

        :param msgExpr: the message expression to send to the destination vertex given a (`src`, `edge`, `dst`) triplet.
                        Source/destination vertex properties and edge properties are nested under columns `src`, `dst`,
                        and `edge`, respectively.
                        You can reference them using :func:`src`, :func:`dst`, and :func:`edge`.
                        Null messages are not included in message aggregation.
        """
        self._java_obj.sendMsgToDst(msgExpr._jc)
        return self

    def aggMsgs(self, aggExpr):
        """
        Defines how messages are aggregated after grouped by target vertex IDs.

        :param aggExpr: the message aggregation expression, such as `sum(Pregel.msg())`.
                        You can reference the message column by :func:`msg` and the vertex ID by `col("id")`,
                        while the latter is usually not used.
        """
        self._java_obj.aggMsgs(aggExpr._jc)
        return self

    def run(self):
        """
        Runs the defined Pregel algorithm.

        :return: the result vertex DataFrame from the final iteration including both original and additional columns.
        """
        return DataFrame(self._java_obj.run(), self.graph.vertices.sql_ctx)

    @staticmethod
    def msg():
        """
        References the message column in aggregating messages and updating additional vertex columns.

        See :func:`aggMsgs` and :func:`withVertexColumn`
        """
        return col("_pregel_msg_")

    @staticmethod
    def src(colName):
        """
        References a source vertex column in generating messages to send.

        See :func:`sendMsgToSrc` and :func:`sendMsgToDst`

        :param colName: the vertex column name.
        """
        return col("src." + colName)

    @staticmethod
    def dst(colName):
        """
        References a destination vertex column in generating messages to send.

        See :func:`sendMsgToSrc` and :func:`sendMsgToDst`

        :param colName: the vertex column name.
        """
        return col("dst." + colName)

    @staticmethod
    def edge(colName):
        """
        References an edge column in generating messages to send.

        See :func:`sendMsgToSrc` and :func:`sendMsgToDst`

        :param colName: the edge column name.
        """
        return col("edge." + colName)
