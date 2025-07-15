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
from typing import Any, Optional, Union

if sys.version > "3":
    basestring = str

from pyspark import SparkContext
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.storagelevel import StorageLevel

from graphframes.lib import Pregel


def _from_java_gf(jgf: Any, spark: SparkSession) -> "GraphFrame":
    """
    (internal) creates a python GraphFrame wrapper from a java GraphFrame.

    :param jgf:
    """
    pv = DataFrame(jgf.vertices(), spark)
    pe = DataFrame(jgf.edges(), spark)
    return GraphFrame(pv, pe)


def _java_api(jsc: SparkContext) -> Any:
    javaClassName = "org.graphframes.GraphFramePythonAPI"
    return (
        jsc._jvm.Thread.currentThread()
        .getContextClassLoader()
        .loadClass(javaClassName)
        .newInstance()
    )


class GraphFrame:
    def __init__(self, v: DataFrame, e: DataFrame) -> None:
        self._vertices = v
        self._edges = e
        self._spark = v.sparkSession
        self._sc = self._spark._sc
        self._jvm_gf_api = _java_api(self._sc)

        self.ID = self._jvm_gf_api.ID()
        self.SRC = self._jvm_gf_api.SRC()
        self.DST = self._jvm_gf_api.DST()
        self._ATTR = self._jvm_gf_api.ATTR()

        # Check that provided DataFrames contain required columns
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

        self._jvm_graph = self._jvm_gf_api.createGraph(v._jdf, e._jdf)

    @property
    def vertices(self) -> DataFrame:
        return self._vertices

    @property
    def edges(self) -> DataFrame:
        return self._edges

    def __repr__(self):
        return self._jvm_graph.toString()

    def cache(self) -> "GraphFrame":
        self._jvm_graph.cache()
        return self

    def persist(self, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY) -> "GraphFrame":
        javaStorageLevel = self._sc._getJavaStorageLevel(storageLevel)
        self._jvm_graph.persist(javaStorageLevel)
        return self

    def unpersist(self, blocking: bool = False) -> "GraphFrame":
        self._jvm_graph.unpersist(blocking)
        return self

    @property
    def outDegrees(self) -> DataFrame:
        jdf = self._jvm_graph.outDegrees()
        return DataFrame(jdf, self._spark)

    @property
    def inDegrees(self) -> DataFrame:
        jdf = self._jvm_graph.inDegrees()
        return DataFrame(jdf, self._spark)

    @property
    def degrees(self) -> DataFrame:
        jdf = self._jvm_graph.degrees()
        return DataFrame(jdf, self._spark)

    @property
    def triplets(self) -> DataFrame:
        jdf = self._jvm_graph.triplets()
        return DataFrame(jdf, self._spark)

    @property
    def pregel(self):
        return Pregel(self)

    def find(self, pattern: str) -> DataFrame:
        jdf = self._jvm_graph.find(pattern)
        return DataFrame(jdf, self._spark)

    def filterVertices(self, condition: Union[str, Column]) -> "GraphFrame":
        if isinstance(condition, basestring):
            jdf = self._jvm_graph.filterVertices(condition)
        elif isinstance(condition, Column):
            jdf = self._jvm_graph.filterVertices(condition._jc)
        else:
            raise TypeError("condition should be string or Column")
        return _from_java_gf(jdf, self._spark)

    def filterEdges(self, condition: Union[str, Column]) -> "GraphFrame":
        if isinstance(condition, basestring):
            jdf = self._jvm_graph.filterEdges(condition)
        elif isinstance(condition, Column):
            jdf = self._jvm_graph.filterEdges(condition._jc)
        else:
            raise TypeError("condition should be string or Column")
        return _from_java_gf(jdf, self._spark)

    def dropIsolatedVertices(self) -> "GraphFrame":
        jdf = self._jvm_graph.dropIsolatedVertices()
        return _from_java_gf(jdf, self._spark)

    def bfs(
        self, fromExpr: str, toExpr: str, edgeFilter: Optional[str] = None, maxPathLength: int = 10
    ) -> DataFrame:
        builder = (
            self._jvm_graph.bfs().fromExpr(fromExpr).toExpr(toExpr).maxPathLength(maxPathLength)
        )
        if edgeFilter is not None:
            builder.edgeFilter(edgeFilter)
        jdf = builder.run()
        return DataFrame(jdf, self._spark)

    def aggregateMessages(
        self,
        aggCol: Union[Column, str],
        sendToSrc: Union[Column, str, None] = None,
        sendToDst: Union[Column, str, None] = None,
    ) -> DataFrame:
        # Check that either sendToSrc, sendToDst, or both are provided
        if sendToSrc is None and sendToDst is None:
            raise ValueError("Either `sendToSrc`, `sendToDst`, or both have to be provided")
        builder = self._jvm_graph.aggregateMessages()
        if sendToSrc is not None:
            if isinstance(sendToSrc, Column):
                builder.sendToSrc(sendToSrc._jc)
            elif isinstance(sendToSrc, basestring):
                builder.sendToSrc(sendToSrc)
            else:
                raise TypeError("Provide message either as `Column` or `str`")
        if sendToDst is not None:
            if isinstance(sendToDst, Column):
                builder.sendToDst(sendToDst._jc)
            elif isinstance(sendToDst, basestring):
                builder.sendToDst(sendToDst)
            else:
                raise TypeError("Provide message either as `Column` or `str`")
        if isinstance(aggCol, Column):
            jdf = builder.agg(aggCol._jc)
        else:
            jdf = builder.agg(aggCol)
        return DataFrame(jdf, self._spark)

    # Standard algorithms

    def connectedComponents(
        self,
        algorithm: str = "graphframes",
        checkpointInterval: int = 2,
        broadcastThreshold: int = 1000000,
        useLabelsAsComponents: bool = False,
    ) -> DataFrame:
        jdf = (
            self._jvm_graph.connectedComponents()
            .setAlgorithm(algorithm)
            .setCheckpointInterval(checkpointInterval)
            .setBroadcastThreshold(broadcastThreshold)
            .setUseLabelsAsComponents(useLabelsAsComponents)
            .run()
        )
        return DataFrame(jdf, self._spark)

    def labelPropagation(self, maxIter: int) -> DataFrame:
        jdf = self._jvm_graph.labelPropagation().maxIter(maxIter).run()
        return DataFrame(jdf, self._spark)

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

    def shortestPaths(self, landmarks: list[Any]) -> DataFrame:
        jdf = self._jvm_graph.shortestPaths().landmarks(landmarks).run()
        return DataFrame(jdf, self._spark)

    def stronglyConnectedComponents(self, maxIter: int) -> DataFrame:
        jdf = self._jvm_graph.stronglyConnectedComponents().maxIter(maxIter).run()
        return DataFrame(jdf, self._spark)

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
        # This call is actually useless, because one needs to build the configuration first...
        builder = self._jvm_graph.svdPlusPlus()
        builder.rank(rank).maxIter(maxIter).minValue(minValue).maxValue(maxValue)
        builder.gamma1(gamma1).gamma2(gamma2).gamma6(gamma6).gamma7(gamma7)
        jdf = builder.run()
        loss = builder.loss()
        v = DataFrame(jdf, self._spark)
        return (v, loss)

    def triangleCount(self) -> DataFrame:
        jdf = self._jvm_graph.triangleCount().run()
        return DataFrame(jdf, self._spark)

    def powerIterationClustering(
        self, k: int, maxIter: int, weightCol: Optional[str] = None
    ) -> DataFrame:
        if weightCol:
            weightCol = self._spark._jvm.scala.Option.apply(weightCol)
        else:
            weightCol = self._spark._jvm.scala.Option.empty()
        jdf = self._jvm_graph.powerIterationClustering(k, maxIter, weightCol)
        return DataFrame(jdf, self._spark)


def _test():
    import doctest

    import graphframe

    globs = graphframe.__dict__.copy()
    globs["sc"] = SparkContext("local[4]", "PythonTest", batchSize=2)
    globs["spark"] = SparkSession(globs["sc"]).builder.getOrCreate()
    (failure_count, test_count) = doctest.testmod(
        globs=globs, optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE
    )
    globs["sc"].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
