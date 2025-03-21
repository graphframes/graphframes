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

import org.apache.spark.annotation.Experimental

import java.util
import scala.jdk.CollectionConverters._
import org.graphframes.GraphFrame
import org.apache.spark.graphx.{lib => graphxlib}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, MapType}

/**
 * Computes shortest paths from every vertex to the given set of landmark vertices. Note that this
 * takes edge direction into account.
 *
 * The returned DataFrame contains all the original vertex information as well as one additional
 * column:
 *   - distances (`MapType[vertex ID type, IntegerType]`): For each vertex v, a map containing the
 *     shortest-path distance to each reachable landmark vertex.
 */
class ShortestPaths private[graphframes] (private val graph: GraphFrame) extends Arguments {
  private var lmarks: Option[Seq[Any]] = None

  /**
   * The list of landmark vertex ids. Shortest paths will be computed to each landmark.
   */
  def landmarks(value: Seq[Any]): this.type = {
    // TODO(tjh) do some initial checks here, without running queries.
    lmarks = Some(value)
    this
  }

  /**
   * The list of landmark vertex ids. Shortest paths will be computed to each landmark.
   */
  def landmarks(value: util.ArrayList[Any]): this.type = {
    landmarks(value.asScala.toSeq)
  }

  def run(): DataFrame = {
    ShortestPaths.run(graph, check(lmarks, "landmarks"))
  }

  /**
   * Experimental implementation without GraphX and with support of undirected graphs
   * @param isDirected
   *   should graph be considered as undirected?
   * @return
   */
  @Experimental
  def runInGraphFrames(isDirected: Boolean = true): DataFrame = {
    // At the moment it is about five times slower compared to GraphX
    ShortestPaths.runWithGraphFrames(graph, check(lmarks, "landmarks"), isDirected)
  }
}

private object ShortestPaths {

  private def run(graph: GraphFrame, landmarks: Seq[Any]): DataFrame = {
    val idType = graph.vertices.schema(GraphFrame.ID).dataType
    val longIdToLandmark = landmarks.map(l => GraphXConversions.integralId(graph, l) -> l).toMap
    val gx = graphxlib.ShortestPaths
      .run(graph.cachedTopologyGraphX, longIdToLandmark.keys.toSeq.sorted)
      .mapVertices { case (_, m) => m.toSeq }
    val g = GraphXConversions.fromGraphX(graph, gx, vertexNames = Seq(DISTANCE_ID))
    val distanceCol: Column = if (graph.hasIntegralIdType) {
      // It seems there are no easy way to convert a sequence of pairs into a map
      val mapToLandmark = udf { distances: Seq[Row] =>
        distances.map { case Row(k: Long, v: Int) =>
          k -> v
        }.toMap
      }
      mapToLandmark(g.vertices(DISTANCE_ID))
    } else {
      val func = new UDF1[Seq[Row], Map[Any, Int]] {
        override def call(t1: Seq[Row]): Map[Any, Int] = {
          t1.map { case Row(k: Long, v: Int) =>
            longIdToLandmark(k) -> v
          }.toMap
        }
      }
      val mapToLandmark = udf(func, MapType(idType, IntegerType, false))
      mapToLandmark(col(DISTANCE_ID))
    }
    val cols = graph.vertices.columns.map(col) :+ distanceCol.as(DISTANCE_ID)
    g.vertices.select(cols.toSeq: _*)
  }

  private def runWithGraphFrames(
      graph: GraphFrame,
      landmarks: Seq[Any],
      isDirected: Boolean = true): DataFrame = {

    def initMap(id: Column): Column = {
      map(landmarks.flatMap(l =>
        Seq(lit(l), when(id === lit(l), lit(0L)).otherwise(lit(null).cast(LongType)))): _*)
    }

    def emptyMap: Column = {
      map(landmarks.flatMap(l => Seq(lit(l), lit(null).cast(LongType))): _*)
    }

    def addMaps(colLeft: Column, colRight: Column) = {
      when(colRight.isNull, colLeft)
        .when(colLeft.isNull, colRight)
        .otherwise(
          map_zip_with(
            colLeft,
            colRight,
            (_, leftDistance, rightDistance) => {
              when(leftDistance.isNull || (leftDistance > rightDistance), rightDistance)
                .otherwise(leftDistance)
            }))
    }

    def incrementMap(mapCol: Column) = {
      transform_values(mapCol, (_, distance) => distance + lit(1L))
    }

    def concatArrayOfMaps(arrayCol: Column) = {
      reduce(arrayCol, emptyMap, addMaps)
    }

    def isDistanceImproved(mapLeft: Column, mapRight: Column) = {
      reduce(
        map_values(
          map_zip_with(
            mapLeft,
            mapRight,
            (_, leftDistance, rightDistance) =>
              (leftDistance.isNotNull && rightDistance.isNull) || (leftDistance < rightDistance))),
        lit(false),
        (left, right) => left || right)
    }

    val pregel = graph.pregel
      .withVertexColumn(
        DISTANCE_ID,
        when(col(GraphFrame.ID).isin(landmarks: _*), initMap(col(GraphFrame.ID)))
          .otherwise(emptyMap),
        addMaps(col(DISTANCE_ID), Pregel.msg))
      .sendMsgToDst(when(
        isDistanceImproved(incrementMap(Pregel.src(DISTANCE_ID)), Pregel.dst(DISTANCE_ID)),
        incrementMap(Pregel.src(DISTANCE_ID))))
      .aggMsgs(concatArrayOfMaps(collect_list(Pregel.msg)))

    if (isDirected) {
      pregel.run()
    } else {
      pregel
        .sendMsgToSrc(
          when(
            isDistanceImproved(incrementMap(Pregel.dst(DISTANCE_ID)), Pregel.src(DISTANCE_ID)),
            incrementMap(Pregel.dst(DISTANCE_ID))))
        .run()
    }
  }

  private val DISTANCE_ID = "distances"

}
