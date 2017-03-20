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

import itertools
import math
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, functions as sqlFuncs, types
from .graphframe import GraphFrame, AggregateMessages as AM

__all__ = ['Graphs', 'BeliefPropagation']


class Graphs(object):
    """
    Example GraphFrames
    """

    def __init__(self, sqlContext):
        """
        :param sqlContext: SQLContext
        """
        self._sql = sqlContext
        self._sc = sqlContext._sc

    def friends(self):
        """
        A GraphFrame of friends in a (fake) social network.
        """
        sqlContext = self._sql
        # Vertex DataFrame
        v = sqlContext.createDataFrame([
            ("a", "Alice", 34),
            ("b", "Bob", 36),
            ("c", "Charlie", 30),
            ("d", "David", 29),
            ("e", "Esther", 32),
            ("f", "Fanny", 36)
        ], ["id", "name", "age"])
        # Edge DataFrame
        e = sqlContext.createDataFrame([
            ("a", "b", "friend"),
            ("b", "c", "follow"),
            ("c", "b", "follow"),
            ("f", "c", "follow"),
            ("e", "f", "follow"),
            ("e", "d", "friend"),
            ("d", "a", "friend")
        ], ["src", "dst", "relationship"])
        # Create a GraphFrame
        return GraphFrame(v, e)

    def gridIsingModel(self, n, vStd=1.0, eStd=1.0):
        """Grid Ising model with random parameters.

        Ising models are probabilistic graphical models over binary variables x,,i,,.
        Each binary variable x,,i,, corresponds to one vertex, and it may take values -1 or +1.
        The probability distribution P(X) (over all x,,i,,) is parameterized by vertex factors
        a,,i,, and edge factors b,,ij,,:
        {{{
         P(X) = (1/Z) * exp[ \sum_i a_i x_i + \sum_{ij} b_{ij} x_i x_j ]
        }}}
        where Z is the normalization constant (partition function). See
        https://en.wikipedia.org/wiki/Ising_model Wikipedia for more information on Ising
        models.

        Each vertex is parameterized by a single scalar a,,i,,.
        Each edge is parameterized by a single scalar b,,ij,,.

        :param n: Length of one side of the grid.  The grid will be of size n x n.
        :param vStd: Standard deviation of normal distribution used to generate vertex factors "a".
                     Default of 1.0.
        :param eStd: Standard deviation of normal distribution used to generate edge factors "b".
                     Default of 1.0.
        :return: GraphFrame. Vertices have columns "id" and "a". Edges have columns "src", "dst",
            and "b".  Edges are directed, but they should be treated as undirected in any algorithms
            run on this model. Vertex IDs are of the form "i,j".  E.g., vertex "1,3" is in the
            second row and fourth column of the grid.
        """
        assert n >= 1,\
            "Grid graph must have size >= 1, but was given invalid value n = {}".format(n)

        # create coodinates grid
        coordinates = self._sql.createDataFrame(
            itertools.product(range(n), range(n)),
            schema=('i', 'j'))

        # create SQL expression for converting coordinates (i,j) to a string ID "i,j"
        # avoid Cartesian join due to SPARK-15425: use generator since n should be small
        toIDudf = sqlFuncs.udf(lambda i, j: '{},{}'.format(i,j))

        # create the vertex DataFrame
        # create SQL expression for converting coordinates (i,j) to a string ID "i,j"
        vIDcol = toIDudf(sqlFuncs.col('i'), sqlFuncs.col('j'))
        # add random parameters generated from a normal distribution
        seed = 12345
        vertices = (coordinates.withColumn('id', vIDcol)
            .withColumn('a', sqlFuncs.randn(seed) * vStd))

        # create the edge DataFrame
        # create SQL expression for converting coordinates (i,j+1) and (i+1,j) to string IDs
        rightIDcol = toIDudf(sqlFuncs.col('i'), sqlFuncs.col('j') + 1)
        downIDcol = toIDudf(sqlFuncs.col('i') + 1, sqlFuncs.col('j'))
        horizontalEdges = (coordinates.filter(sqlFuncs.col('j') != n - 1)
            .select(vIDcol.alias('src'), rightIDcol.alias('dst')))
        verticalEdges = (coordinates.filter(sqlFuncs.col('i') != n - 1)
            .select(vIDcol.alias('src'), downIDcol.alias('dst')))
        allEdges = horizontalEdges.unionAll(verticalEdges)
        # add random parameters from a normal distribution
        edges = allEdges.withColumn('b', sqlFuncs.randn(seed + 1) * eStd)

        # create the GraphFrame
        g = GraphFrame(vertices, edges)

        # materialize graph as workaround for SPARK-13333
        g.vertices.cache().count()
        g.edges.cache().count()

        return g


class BeliefPropagation(object):

    @classmethod
    def main(cls):
        conf = SparkConf().setAppName("BeliefPropagation example")
        sc = SparkContext.getOrCreate(conf)
        sql = SQLContext.getOrCreate(sc)

        # create graphical model g of size 3 x 3
        g = Graphs(sql).gridIsingModel(3)
        print("Original Ising model:")
        g.vertices.show()
        g.edges.show()

        # run BP for 5 iterations
        numIter = 5
        results = cls.runBPwithGraphFrames(g, numIter)

        # display beliefs
        beliefs = results.vertices.select('id', 'belief')
        print("Done with BP. Final beliefs after {} iterations:".format(numIter))
        beliefs.show()

        sc.stop()

    @staticmethod
    def colorGraph(g):
        """Given a GraphFrame, choose colors for each vertex.

        No neighboring vertices will share the same color. The number of colors is minimized.

        This is written specifically for grid graphs. For non-grid graphs, it should be generalized,
        such as by using a greedy coloring scheme.

        :param g: Grid graph generated by :meth:`Graphs.gridIsingModel()`
        :return: Same graph, but with a new vertex column "color" of type Int (0 or 1)

        """

        colorUDF = sqlFuncs.udf(
            lambda i, j: 0 if (i + j) % 2 == 0 else 1,
            returnType=types.IntegerType())
        v = g.vertices.withColumn('color', colorUDF(sqlFuncs.col('i'), sqlFuncs.col('j')))
        return GraphFrame(v, g.edges)

    @classmethod
    def runBPwithGraphFrames(cls, g, numIter):
        """Run Belief Propagation using GraphFrame.

        This implementation of BP shows how to use GraphFrame's aggregateMessages method.
        """
        # choose colors for vertices for BP scheduling
        colorG = cls.colorGraph(g)
        numColors = colorG.vertices.select('color').distinct().count()

        # TODO: handle vertices without any edges

        # initialize vertex beliefs at 0.0
        gx = GraphFrame(colorG.vertices.withColumn('belief', sqlFuncs.lit(0.0)), colorG.edges)

        # run BP for numIter iterations
        for iter_ in range(numIter):
            # for each color, have that color receive messages from neighbors
            for color in range(numColors):
                # Send messages to vertices of the current color.
                # We may send to source or destination since edges are treated as undirected.
                msgForSrc = sqlFuncs.when(
                    AM.src()['color'] == color,
                    AM.edge()['b'] * AM.dst()['belief'])
                msgForDst = sqlFuncs.when(
                    AM.dst()['color'] == color,
                    AM.edge()['b'] * AM.src()['belief'])
                # numerically stable sigmoid
                logistic = sqlFuncs.udf(cls._sigmoid, returnType=types.DoubleType())
                aggregates = gx.aggregateMessages(
                    sqlFuncs.sum(AM.msg()).alias("aggMess"),
                    msgToSrc=msgForSrc,
                    msgToDst=msgForDst)
                v = gx.vertices
                # receive messages and update beliefs for vertices of the current color
                newBeliefCol = sqlFuncs.when(
                    (v['color'] == color) & (aggregates['aggMess'].isNotNull()),
                    logistic(aggregates['aggMess'] + v['a'])
                ).otherwise(v['belief'])  # keep old beliefs for other colors
                newVertices = (v
                    .join(aggregates, on=(v['id'] == aggregates['id']), how='left_outer')
                    .drop(aggregates['id'])  # drop duplicate ID column (from outer join)
                    .withColumn('newBelief', newBeliefCol)  # compute new beliefs
                    .drop('aggMess')  # drop messages
                    .drop('belief')  # drop old beliefs
                    .withColumnRenamed('newBelief', 'belief')
                )
                # cache new vertices using workaround for SPARK-1334
                cachedNewVertices = AM.getCachedDataFrame(newVertices)
                gx = GraphFrame(cachedNewVertices, gx.edges)

        # Drop the "color" column from vertices
        return GraphFrame(gx.vertices.drop('color'), gx.edges)


    @staticmethod
    def _sigmoid(x):
        """Numerically stable sigmoid function 1 / (1 + exp(-x))"""
        if not x:
            return None
        if x >= 0:
            z = math.exp(-x)
            return 1 / (1 + z)
        else:
            z = math.exp(x)
            return z / (1 + z)
