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

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.graphframes.GraphFrame

class Graphs {
  // Note: these cannot be values: we are creating and destroying spark contexts during the tests, and turning
  // these into vals means we would hold onto a potentially destroyed spark context.
  private def sc: SparkContext = SparkContext.getOrCreate()
  private def sqlContext: SQLContext = SQLContext.getOrCreate(sc)

  /**
   * Graph of friends in a social network.
   */
  def friends: GraphFrame = {
    // For the same reason as above, this cannot be a value.
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

  /**
   * Two densely connected blobs (vertices 0->n-1 and n->2n-1) connected by a single edge (0->n)
   * @param blobSize the size of each blob.
   * @return
   */
  def twoBlobs(blobSize: Int): GraphFrame = {
    val n = blobSize
    val edges1 = for (v1 <- 0 until n; v2 <- 0 until n) yield (v1.toLong, v2.toLong, s"$v1-$v2")
    val edges2 = for {
      v1 <- n until (2 * n)
      v2 <- n until (2 * n) } yield (v1.toLong, v2.toLong, s"$v1-$v2")
    val edges = edges1 ++ edges2 :+ (0L, n.toLong, s"0-$n")
    val vertices = (0 until (2 * n)).map { v => (v.toLong, s"$v", v) }
    val e = sqlContext.createDataFrame(edges).toDF("src", "dst", "e_attr1")
    val v = sqlContext.createDataFrame(vertices).toDF("id", "v_attr1", "v_attr2")
    GraphFrame(v, e)
  }

  /**
   * A star graph, with a central element indexed 0 (the root) and the n other leaf vertices.
   * @param n the number of leaves
   * @return
   */
  def star(n: Int): GraphFrame = {
    val vertices = sqlContext.createDataFrame(Seq((0, "root")) ++ (1 to n).map { i =>
      (i, s"node-$i")
    }).toDF("id", "v_attr1")
    val edges = sqlContext.createDataFrame((1 to n).map { i =>
      (i, 0, s"edge-$i")
    }).toDF("src", "dst", "e_attr1")
    GraphFrame(vertices, edges)
  }

  /**
   * Some synthetic data that sits in Spark.
   *
   * No description available.
   * @return
   */
  def ALSSyntheticData(): GraphFrame = {
    val sc = sqlContext.sparkContext
    val data = sc.parallelize(als_data).map { line =>
      val fields = line.split(",")
      (fields(0).toLong * 2, fields(1).toLong * 2 + 1, fields(2).toDouble)
    }
    val edges = sqlContext.createDataFrame(data).toDF("src", "dst", "weight")
    val vs = data.flatMap(r => r._1 :: r._2 :: Nil).collect().distinct.map(x => Tuple1(x))
    val vertices = sqlContext.createDataFrame(vs).toDF("id")
    GraphFrame(vertices, edges)
  }

  private lazy val als_data =
    """
      |1,1,5.0
      |1,2,1.0
      |1,3,5.0
      |1,4,1.0
      |2,1,5.0
      |2,2,1.0
      |2,3,5.0
      |2,4,1.0
      |3,1,1.0
      |3,2,5.0
      |3,3,1.0
      |3,4,5.0
      |4,1,1.0
      |4,2,5.0
      |4,3,1.0
      |4,4,5.0
    """.stripMargin.split("\n").map(_.trim).filterNot(_.isEmpty)

}

/** Example GraphFrames for testing the API */
object Graphs extends Graphs
