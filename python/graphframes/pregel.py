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

from pyspark import since, SparkContext
from pyspark.sql.functions import col
from pyspark.ml.common import _java2py, _py2java
from pyspark.ml.wrapper import JavaWrapper, _jvm


class Pregel(JavaWrapper):
    """
    This class implement pregel on GraphFrame.
    We can get the Pregel instance by `graphFrame.pregel`, or construct it via a `graph`
    argument. and call a series of methods, then call method `run` to start pregel.
    It will return a DataFrame which is the vertices dataframe generated in the last round.
    When pregel `run` start, first, it will initialize some columns in vertices dataframe,
    which defined by `withVertexColumn` (user can call it multiple times),
    and then start iteration.
    Once the iteration start, in each supersteps of pregel, include 3 phases:
    phase-1) generate the `triplets` dataframe, and generate the “msg” to send.
    The target vertex to send and the msg is set via `sendMsg` method.
    phase-2) Do msg aggregation. It will use the aggregation column which is set via
    `aggMsgs` method. Each vertex aggregates those messages which it receives.
    Now vertices dataframe owns a new column which value is the aggregated msgs
    (received by each vertex). Now update vertex property columns, the update expressions
    is set by `updateVertexColumn` method and return the new vertices dataframe.
    The pregel iteration will run `maxIter` time, which can be set via `setMaxIter` method.

    :param gf :class:`GraphFrame` holding a graph with vertices and edges stored as DataFrames.

    >>> from .graphframe import GraphFrame, Pregel
    >>> from pyspark.sql.functions import coalesce, col, lit, sum, when
    >>> edges = spark.createDataFrame([[0L, 1L],
    ...                                [1L, 2L],
    ...                                [2L, 4L],
    ...                                [2L, 0L],
    ...                                [3L, 4L], # 3 has no in-links
    ...                                [4L, 0L],
    ...                                [4L, 2L]], ["src", "dst"])
    >>> edges.cache()
    >>> vertices = spark.createDataFrame([[0L], [1L], [2L], [3L], [4L]], ["id"])
    >>> numVertices = vertices.count()
    >>> vertices = GraphFrame(vertices, edges).outDegrees
    >>> graph = GraphFrame(vertices, edges)
    >>> alpha = 0.15
    >>> pageRankResultDF = graph.pregel \
    ...   .setMaxIter(5) \
    ...   .withVertexColumn("rank", lit(1.0 / numVertices),
    ...                     coalesce(Pregel.msg(),
    ...                              lit(0.0)) * lit(1.0 - alpha) + lit(alpha / numVertices)) \
    ...   .sendMsgToDst(Pregel.src("rank") / Pregel.src("outDegree")) \
    ...   .aggMsgs(sum(Pregel.msg())) \
    ...   .run()
    """

    def __init__(self, gf):
        super(Pregel, self).__init__()
        self._java_obj = self._new_java_obj("org.graphframes.Pregel", gf._jvm_graph)

    def setMaxIter(self, value):
        """
        Set max iteration number for the pregel running. Default value is 10.
        """
        self._java_obj.setMaxIter(int(value))
        return self

    def setCheckpointInterval(self, value):
        """
        Set the period to do the checkpoint when running pregel.
        If set to zero, then do not checkpoint.
        Negative value is not allowed.
        Default value is 2.
        """
        self._java_obj.setCheckpointInterval(int(value))
        return self

    def withVertexColumn(self, colName, initialExpr, updateAfterAggMsgsExpr):
        """
        Use this method to set those vertex columns which will be initialized before
        pregel rounds start, and these columns will be updated after vertex receive
        aggregated messages in each round.

        :param colName: The column name of initialized column in vertex Dataframe.
        :param initialExpr: The column expression used to initialize the column.
        :param updateAfterAggMsgsExpr: The column expression used to update the column.
                                       Note that this sql expression can reference all
                                       vertex columns and an extra message column
                                       `Pregel.msg`. If the vertex receive no messages,
                                       The msg column will be null, otherwise will be
                                       the aggregated result of all received messages.
        """
        self._java_obj.withVertexColumn(colName, initialExpr._jc, updateAfterAggMsgsExpr._jc)
        return self

    def sendMsgToSrc(self, msgExpr):
        """
        Set the message column. In each round of pregel, each triplet
        (src-edge-dst) will generate zero or one message and the message will
        be sent to the src vertex of this triplet.

        :param msgExpr: The message expression. It is a sql expression and it
                        can reference all propertis in the triplet, in the way
                        `Pregel.src("src_col_name)`, `Pregel.edge("edge_col_name)`,
                        `Pregel.dst("dst_col_name)`. If `msgExpr` is null, pregel
                        will not send message.
        """
        self._java_obj.sendMsgToSrc(msgExpr._jc)
        return self

    def sendMsgToDst(self, msgExpr):
        """
        Set the message column. In each round of pregel, each triplet
        (src-edge-dst) will generate zero or one message and the message will
        be sent to the dst vertex of this triplet.

        :param msgExpr: The message expression. It is a sql expression and it
                        can reference all propertis in the triplet, in the way
                        `Pregel.src("src_col_name)`, `Pregel.edge("edge_col_name)`,
                        `Pregel.dst("dst_col_name)`. If `msgExpr` is null, pregel
                        will not send message.
        """
        self._java_obj.sendMsgToDst(msgExpr._jc)
        return self

    def aggMsgs(self, aggExpr):
        """
        Set the aggregation expression which used to aggregate messages which received
        by each vertex.

        :param aggExpr: The aggregation expression, such as `sum(Pregel.msgCol)`
        """
        self._java_obj.aggMsgs(aggExpr._jc)
        return self

    def run(self):
        """
        After set a series of things via above methods, then call this method to run
        pregel, and it will return the result vertex dataframe, which will include all
        updated columns in the final rounds of pregel.

        :return: The result vertex dataframe including original and additional columns.
        """
        sc = SparkContext._active_spark_context
        return _java2py(sc, self._java_obj.run())

    @staticmethod
    def msg():
        """
        The message column. `Pregel.aggMsgs` method argument and `Pregel.updateVertexColumn`
        argument `col` can reference this message column.
        """
        return col("_pregel_msg_")

    @staticmethod
    def src(colName):
        """
        construct the column from src vertex columns.
        This column can only be used in the message sql expression.

        :param colName: the column name in the vertex columns.
        """
        return col("src." + colName)

    @staticmethod
    def dst(colName):
        """
        construct the column from dst vertex columns.
        This column can only be used in the message sql expression.

        :param colName: the column name in the edge columns.
        """
        return col("dst." + colName)

    @staticmethod
    def edge(colName):
        """
        construct the column from edge columns.
        This column can only be used in the message sql expression.

        :param colName: the column name in the edge columns.
        """
        return col("edge." + colName)
