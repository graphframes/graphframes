/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.dfgraph.lib


import org.apache.spark.graphx.{Edge, lib => graphxlib}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import com.databricks.dfgraph.DFGraph

/**
 * Implementation of the SVD++ algorithm, based on "Factorization Meets the Neighborhood:
 * a Multifaceted Collaborative Filtering Model",
 * available at [[https://movie-datamining.googlecode.com/svn/trunk/kdd08koren.pdf]].
 */
object SVDPlusPlus {

  /**
   * Implement SVD++ based on "Factorization Meets the Neighborhood:
   * a Multifaceted Collaborative Filtering Model",
   * available at [[https://movie-datamining.googlecode.com/svn/trunk/kdd08koren.pdf]].
   *
   * The prediction rule is rui = u + bu + bi + qi*(pu + |N(u)|^^-0.5^^*sum(y)),
   * see the details on page 6.
   *
   * @param conf SVDPlusPlus parameters
   *
   * @return a graph with vertex attributes containing the trained model
   */
  def run(graph: DFGraph, conf: Conf): (DFGraph, Double) = {
    val edges = graph.edges.select(DFGraph.SRC, DFGraph.DST, COLUMN_WEIGHT).map {
      case Row(src: Long, dst: Long, w: Double) => Edge(src, dst, w)
    }
    val (gx, res) = graphxlib.SVDPlusPlus.run(edges, conf.toGraphXConf)
    val dfg = GraphXConversions.fromGraphX(graph, gx,
      vertexNames = Seq(COLUMN1, COLUMN2, COLUMN3, COLUMN4),
      edgeName = Seq(ECOLUMN1))
    (dfg, res)
  }

  /**
   * Configuration parameters for SVD++.
   *
   *
   *
   * @param rank
   * @param maxIters
   * @param minVal
   * @param maxVal
   * @param gamma1
   * @param gamma2
   * @param gamma6
   * @param gamma7
   */
  // TODO(tjh) put some documentation for the parameters.
  case class Conf(
      rank: Int,
      maxIters: Int,
      minVal: Double,
      maxVal: Double,
      gamma1: Double,
      gamma2: Double,
      gamma6: Double,
      gamma7: Double) extends Serializable {

    /**
     * Convenience conversion method.
     */
    private[dfgraph] def toGraphXConf: graphxlib.SVDPlusPlus.Conf = {
      new graphxlib.SVDPlusPlus.Conf(
        rank = rank,
        maxIters = maxIters,
        minVal = minVal,
        maxVal = maxVal,
        gamma1 = gamma1,
        gamma2 = gamma2,
        gamma6 = gamma6,
        gamma7 = gamma7)
    }
  }

  object Conf {

    /**
     * Default settings for the parameters.
     */
    // TODO(tjh) expose as part of the API
    // TODO(tjh) explain the values of the parameters?
    private[dfgraph] val default = new Conf(10, 2, 0.0, 5.0, 0.007, 0.007, 0.005, 0.015)

    /**
     * Convenience conversion method for users of GraphX.
     * @param conf
     */
    def buildFrom(conf: graphxlib.SVDPlusPlus.Conf): Conf = {
      new Conf(
        rank = conf.rank,
        maxIters = conf.maxIters,
        minVal = conf.minVal,
        maxVal = conf.maxVal,
        gamma1 = conf.gamma1,
        gamma2 = conf.gamma2,
        gamma6 = conf.gamma6,
        gamma7 = conf.gamma7)
    }
  }

  private val COLUMN_WEIGHT = "weight"

  private val COLUMN1 = "column1"
  private val COLUMN2 = "column2"
  private val COLUMN3 = "column3"
  private val COLUMN4 = "column4"
  private val ECOLUMN1 = "ecolumn1"

  private val field1 = StructField(COLUMN1, ArrayType(DoubleType), nullable = false)
  private val field2 = StructField(COLUMN2, ArrayType(DoubleType), nullable = false)
  private val field3 = StructField(COLUMN3, DoubleType, nullable = false)
  private val field4 = StructField(COLUMN4, DoubleType, nullable = false)
  private val eField1 = StructField(ECOLUMN1, DoubleType, nullable = false)

  class Builder private[dfgraph] (graph: DFGraph) extends Arguments {
    private var conf: Option[Conf] = None
    private var _loss: Option[Double] = None

    def setConf(c: Conf): this.type = {
      conf = Some(c)
      this
    }

    def run(): DFGraph = {
      val (g, l) = SVDPlusPlus.run(graph, check(conf, "conf"))
      _loss = Some(l)
      g
    }

    def loss: Double = {
      // We could use types instead to make sure that it is never accessed before being run.
      _loss.getOrElse(throw new Exception("The algorithm has not been run yet"))
    }
  }
}
