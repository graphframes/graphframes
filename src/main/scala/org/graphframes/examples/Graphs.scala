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

package org.graphframes.examples

import org.graphframes.GraphFrame

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/** Example GraphFrames for testing the API */
object Graphs {

  private lazy val sc: SparkContext = SparkContext.getOrCreate()
  private lazy val sqlContext: SQLContext = SQLContext.getOrCreate(sc)

  /**
   * Graph of friends in a social network.
   */
  lazy val friends: GraphFrame = {
    // Vertex DataFrame
    val v = sqlContext.createDataFrame(List(
      ("a", "Alice", 34),
      ("b", "Bob", 36),
      ("c", "Charlie", 30),
      ("d", "David", 29),
      ("e", "Esther", 32),
      ("f", "Fanny", 36)
    )).toDF("id", "name", "age")
    // Edge DataFrame
    val e = sqlContext.createDataFrame(List(
      ("a", "b", "friend"),
      ("b", "c", "follow"),
      ("c", "b", "follow"),
      ("f", "c", "follow"),
      ("e", "f", "follow"),
      ("e", "d", "friend"),
      ("d", "a", "friend")
    )).toDF("src", "dst", "relationship")
    // Create a GraphFrame
    GraphFrame(v, e)
  }

}
