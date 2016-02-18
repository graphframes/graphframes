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
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import org.graphframes.GraphFrame

/**
 * Implement SVD++ based on "Factorization Meets the Neighborhood:
 * a Multifaceted Collaborative Filtering Model",
 * available at [[https://movie-datamining.googlecode.com/svn/trunk/kdd08koren.pdf]].
 *
 * The prediction rule is rui = u + bu + bi + qi*(pu + |N(u)|^^-0.5^^*sum(y)),
 * see the details on page 6.
 *
 * Configuration parameters: see the description of each parameter in the article.
 *
 * @return a graph with vertex attributes containing the trained model
 */
class SVDPlusPlus private[graphframes] (private val graph: GraphFrame) extends Arguments {
  private var _rank: Int = 10
  private var _maxIters: Int = 2
  private var _minVal: Double = 0.0
  private var _maxVal: Double = 5.0
  private var _gamma1: Double = 0.007
  private var _gamma2: Double = 0.007
  private var _gamma6: Double = 0.005
  private var _gamma7: Double = 0.015

  private var _loss: Option[Double] = None

  def rank(rank: Int): this.type = {
    _rank = rank
    this
  }

  def maxIterations(iter: Int): this.type = {
    _maxIters = iter
    this
  }

  def minValue(minVal: Double): this.type = {
    _minVal = minVal
    this
  }

  def maxValue(maxVal: Double): this.type = {
    _maxVal =maxVal
    this
  }

  def gamma1(gamma1: Double): this.type = {
    _gamma1 = gamma1
    this
  }

  def gamma2(gamma2: Double): this.type = {
    _gamma2 = gamma2
    this
  }

  def gamma6(gamma6: Double): this.type = {
    _gamma6 = gamma6
    this
  }

  def gamma7(gamma7: Double): this.type = {
    _gamma7 = gamma7
    this
  }

  def run(): GraphFrame = {
    val conf = new graphxlib.SVDPlusPlus.Conf(
      rank = _rank,
      maxIters = _maxIters,
      minVal = _minVal,
      maxVal = _maxVal,
      gamma1 = _gamma1,
      gamma2 = _gamma2,
      gamma6 = _gamma6,
      gamma7 = _gamma7)

    val (g, l) = SVDPlusPlus.run(graph, conf)
    _loss = Some(l)
    g
  }

  def loss: Double = {
    // We could use types instead to make sure that it is never accessed before being run.
    _loss.getOrElse(throw new Exception("The algorithm has not been run yet"))
  }
}


/**
 * Implementation of the SVD++ algorithm, based on "Factorization Meets the Neighborhood:
 * a Multifaceted Collaborative Filtering Model",
 * available at [[https://movie-datamining.googlecode.com/svn/trunk/kdd08koren.pdf]].
 */
private object SVDPlusPlus {

  private def run(graph: GraphFrame, conf: graphxlib.SVDPlusPlus.Conf): (GraphFrame, Double) = {
    val edges = graph.edges.select(GraphFrame.SRC, GraphFrame.DST, COLUMN_WEIGHT).map {
      case Row(src: Long, dst: Long, w: Double) => Edge(src, dst, w)
    }
    val (gx, res) = graphxlib.SVDPlusPlus.run(edges, conf)
    val gf = GraphXConversions.fromGraphX(graph, gx,
      vertexNames = Seq(COLUMN1, COLUMN2, COLUMN3, COLUMN4),
      edgeNames = Seq(ECOLUMN1))
    (gf, res)
  }


  private val COLUMN_WEIGHT = "weight"

  private val COLUMN1 = "column1"
  private val COLUMN2 = "column2"
  private val COLUMN3 = "column3"
  private val COLUMN4 = "column4"
  private val ECOLUMN1 = "ecolumn1"

}
