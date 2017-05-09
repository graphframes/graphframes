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

import org.apache.spark.sql.Row

import scala.collection.mutable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.graphframes.examples.Graphs
import org.graphframes.{GraphFrame, GraphFrameTestSparkContext, SparkFunSuite, TestUtils}


class AggregateMessagesSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  test("aggregateMessages") {
    val AM = AggregateMessages
    val g = Graphs.friends
    // For each user, sum the ages of the adjacent users,
    // plus 1 for the src's sum if the edge is "friend".
    val msgToSrc = AM.dst("age") +
      when(AM.edge("relationship") === "friend", lit(1)).otherwise(0)
    val msgToDst = AM.src("age")
    val agg = g.aggregateMessages
      .sendToSrc(msgToSrc)
      .sendToDst(msgToDst)
      .agg(sum(AM.msg).as("summedAges"))
    // Convert agg to a Map.
    import org.apache.spark.sql._
    val aggMap: Map[String, Long] = agg.select("id", "summedAges").collect().map {
      case Row(id: String, s: Long) => id -> s
    }.toMap
    // Compute the truth via brute force for comparison.
    val trueAgg: Map[String, Int] = {
      val user2age = g.vertices.select("id", "age").collect().map {
        case Row(id: String, age: Int) => id -> age
      }.toMap
      val a = mutable.HashMap.empty[String, Int]
      g.edges.select("src", "dst", "relationship").collect().foreach {
        case Row(src: String, dst: String, relationship: String) =>
          a.put(src, a.getOrElse(src, 0) + user2age(dst) + (if (relationship == "friend") 1 else 0))
          a.put(dst, a.getOrElse(dst, 0) + user2age(src))
      }
      a.toMap
    }
    // Compare to the true values.
    aggMap.keys.foreach { case user =>
      assert(aggMap(user) === trueAgg(user), s"Failure on user $user")
    }
    // Perfom the aggregation again, this time providing the messages as Strings instead.
    val msgToSrc2 = "(dst['age'] + CASE WHEN (edge['relationship'] = 'friend') THEN 1 ELSE 0 END)"
    val msgToDst2 = "src['age']"
    val agg2 = g.aggregateMessages
      .sendToSrc(msgToSrc2)
      .sendToDst(msgToDst2)
      .agg("sum(MSG) AS `summedAges`")
    // Convert agg2 to a Map.
    val agg2Map: Map[String, Long] = agg2.select("id", "summedAges").collect().map {
      case Row(id: String, s: Long) => id -> s
    }.toMap
    // Compare to the true values.
    agg2Map.keys.foreach { case user =>
      assert(agg2Map(user) === trueAgg(user), s"Failure on user $user")
    }
  }

  test("aggregateMessages with multiple message and aggregation columns") {
    val AM = AggregateMessages
    val vertices = sqlContext.createDataFrame(
      List((1, 30, 3), (2, 40, 4), (3, 50, 5), (4, 60, 6))).toDF("id", "att1", "att2")
    val edges = sqlContext.createDataFrame(List(1 -> 2, 2 -> 3, 1 -> 4)).toDF("src", "dst")
    val expectedValues = Map(1 -> (100l, 5.0), 2 -> (80l, 4.0), 3 -> (40l, 4.0), 4 -> (30l, 3.0))

    val g = GraphFrame(vertices, edges)
    val agg = g.aggregateMessages
      .sendToDst(AM.src("att1"))
      .sendToSrc(AM.dst("att1"))
      .sendToDst(AM.src("att2"))
      .sendToSrc(AM.dst("att2"))
      .agg(
        sum(AM.msg("att1")).as("sum_att1"),
        avg(AM.msg("att2")).as("avg_att2"))

    //validate schema
    assert(agg.schema.size === 3)
    TestUtils.checkColumnType(agg.schema, "id", IntegerType)
    TestUtils.checkColumnType(agg.schema, "sum_att1", LongType)
    TestUtils.checkColumnType(agg.schema, "avg_att2", DoubleType)

    //validate content
    assert(agg.collect().map { case Row(id: Int, sumAtt1: Long, avgAtt2: Double) =>
      id -> (sumAtt1, avgAtt2)
    }.toMap === expectedValues)
  }
}
