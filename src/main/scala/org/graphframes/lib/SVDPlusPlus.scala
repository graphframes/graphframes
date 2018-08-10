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

package org.graphframes.lib

import org.apache.spark.graphx.{Edge, lib => graphxlib}
import org.apache.spark.sql.{DataFrame, Row}

import org.graphframes.{GraphFrame, Logging}

/**
 * Implement SVD++ based on "Factorization Meets the Neighborhood:
 * a Multifaceted Collaborative Filtering Model",
 * available at [[https://dl.acm.org/citation.cfm?id=1401944]].
 *
 * Note: The status of this algorithm is EXPERIMENTAL. Its API and implementation may be changed
 * in the future.
 *
 * The prediction rule is r,,ui,, = u + b,,u,, + b,,i,, + q,,i,,*(p,,u,, + |N(u)|^^-0.5^^*sum(y)).
 * See the details on page 6 of the article.
 *
 * Configuration parameters: see the description of each parameter in the article.
 *
 * Returns a DataFrame with vertex attributes containing the trained model.  See the object
 * (static) members for the names of the output columns.
 *
 */
class SVDPlusPlus private[graphframes] (private val graph: GraphFrame) extends Arguments {
  private var _rank: Int = 10
  private var _maxIter: Int = 2
  private var _minVal: Double = 0.0
  private var _maxVal: Double = 5.0
  private var _gamma1: Double = 0.007
  private var _gamma2: Double = 0.007
  private var _gamma6: Double = 0.005
  private var _gamma7: Double = 0.015

  private var _loss: Option[Double] = None

  def rank(value: Int): this.type = {
    _rank = value
    this
  }

  def maxIter(value: Int): this.type = {
    _maxIter = value
    this
  }

  def minValue(value: Double): this.type = {
    _minVal = value
    this
  }

  def maxValue(value: Double): this.type = {
    _maxVal = value
    this
  }

  def gamma1(value: Double): this.type = {
    _gamma1 = value
    this
  }

  def gamma2(value: Double): this.type = {
    _gamma2 = value
    this
  }

  def gamma6(value: Double): this.type = {
    _gamma6 = value
    this
  }

  def gamma7(value: Double): this.type = {
    _gamma7 = value
    this
  }

  def run(): DataFrame = {
    val conf = new graphxlib.SVDPlusPlus.Conf(
      rank = _rank,
      maxIters = _maxIter,
      minVal = _minVal,
      maxVal = _maxVal,
      gamma1 = _gamma1,
      gamma2 = _gamma2,
      gamma6 = _gamma6,
      gamma7 = _gamma7)

    val (df, l) = SVDPlusPlus.run(graph, conf)
    _loss = Some(l)
    df
  }

  def loss: Double = {
    // We could use types instead to make sure that it is never accessed before being run.
    _loss.getOrElse(throw new Exception("The algorithm has not been run yet"))
  }
}

object SVDPlusPlus {

  private def run(graph: GraphFrame, conf: graphxlib.SVDPlusPlus.Conf): (DataFrame, Double) = {
    val edges = graph.edges.select(GraphFrame.SRC, GraphFrame.DST, COLUMN_WEIGHT).rdd.map {
      case Row(src: Long, dst: Long, w: Double) => Edge(src, dst, w)
    }
    val (gx, res) = graphxlib.SVDPlusPlus.run(edges, conf)
    val gf = GraphXConversions.fromGraphX(graph, gx,
      vertexNames = Seq(COLUMN1, COLUMN2, COLUMN3, COLUMN4))
    (gf.vertices, res)
  }

  /**
   * Name for input edge DataFrame column containing edge weights.
   *
   * Note: This column name may change in the future!
   */
  val COLUMN_WEIGHT = "weight"

  /**
   * Name for output vertexDataFrame column containing first parameter of learned model,
   * of type `Array[Double]`.
   *
   * Note: This column name may change in the future!
   */
  val COLUMN1 = "column1"

  /**
   * Name for output vertexDataFrame column containing second parameter of learned model,
   * of type `Array[Double]`.
   *
   * Note: This column name may change in the future!
   */
  val COLUMN2 = "column2"

  /**
   * Name for output vertexDataFrame column containing third parameter of learned model,
   * of type `Double`.
   *
   * Note: This column name may change in the future!
   */
  val COLUMN3 = "column3"

  /**
   * Name for output vertexDataFrame column containing fourth parameter of learned model,
   * of type `Double`.
   *
   * Note: This column name may change in the future!
   */
  val COLUMN4 = "column4"
}
