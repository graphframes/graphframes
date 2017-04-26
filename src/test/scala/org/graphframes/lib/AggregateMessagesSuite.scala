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

import scala.collection.mutable

import org.apache.spark.sql.functions._

import org.graphframes.examples.Graphs
import org.graphframes.{GraphFrameTestSparkContext, SparkFunSuite}


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
}
