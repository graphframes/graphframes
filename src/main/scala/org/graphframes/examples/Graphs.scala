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
import org.apache.spark.sql.functions.{col, randn, udf}

import org.graphframes.GraphFrame

class Graphs {
  // Note: these cannot be values: we are creating and destroying spark contexts during the tests,
  // and turning these into vals means we would hold onto a potentially destroyed spark context.
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

  /**
   * This method generates a grid Ising model with random parameters.
   *
   * Ising models are probabilistic graphical models over binary variables x,,i,,.
   * Each binary variable x,,i,, corresponds to one vertex, and it may take values -1 or +1.
   * The probability distribution P(X) (over all x,,i,,) is parameterized by vertex factors a,,i,,
   * and edge factors b,,ij,,:
   * {{{
   *  P(X) = (1/Z) * exp[ \sum_i a_i x_i + \sum_{ij} b_{ij} x_i x_j ]
   * }}}
   * where Z is the normalization constant (partition function). See
   * [[https://en.wikipedia.org/wiki/Ising_model Wikipedia]] for more information on Ising models.
   *
   * Each vertex is parameterized by a single scalar a,,i,,.
   * Each edge is parameterized by a single scalar b,,ij,,.
   *
   * @param  n  Length of one side of the grid.  The grid will be of size n x n.
   * @param  vStd  Standard deviation of normal distribution used to generate vertex factors "a".
   *               Default of 1.0.
   * @param  eStd  Standard deviation of normal distribution used to generate edge factors "b".
   *               Default of 1.0.
   * @return  GraphFrame.  Vertices have columns "id" and "a".
   *          Edges have columns "src", "dst", and "b".
   *          Vertex IDs are of the form "i,j".  E.g., vertex "1,3" is in the second row and fourth
   *          column of the grid.
   */
  def gridIsingModel(sqlContext: SQLContext, n: Int, vStd: Double, eStd: Double): GraphFrame = {
    require(n >= 1, s"Grid graph must have size >= 1, but was given invalid value n = $n")
    val rows = sqlContext.createDataFrame(Range(0, n).map(i => Tuple1(i))).toDF("i")
    val cols = rows.select(rows("i").as("j"))
    // Cartesian join to create grid
    val coordinates = rows.join(cols)

    // Create SQL expression for converting coordinates (i,j) to a string ID "i,j"
    val toIDudf = udf { (i: Int, j: Int) => i.toString + "," + j.toString }

    // Create the vertex DataFrame
    //  Create SQL expression for converting coordinates (i,j) to a string ID "i,j"
    val vIDcol = toIDudf(col("i"), col("j"))
    //  Add random parameters generated from a normal distribution
    val seed = 12345
    val vertices = coordinates.withColumn("id", vIDcol)  // vertex IDs "i,j"
      .withColumn("a", randn(seed) * vStd)  // Ising parameter for vertex

    // Create the edge DataFrame
    //  Create SQL expression for converting coordinates (i,j+1) and (i+1.j) to string IDs
    val rightIDcol = toIDudf(col("i"), col("j") + 1)
    val downIDcol = toIDudf(col("i") + 1, col("j"))
    val horizontalEdges = coordinates.filter(col("j") !== n - 1)
      .select(vIDcol.as("src"), rightIDcol.as("dst"))
    val verticalEdges = coordinates.filter(col("i") !== n - 1)
      .select(vIDcol.as("src"), downIDcol.as("dst"))
    val hvEdges = horizontalEdges.unionAll(verticalEdges)
    //  These edges are directed.  We duplicate and reverse them to create bidirectional edges.
    val reversedEdges = hvEdges.select(col("src").as("dst"), col("dst").as("src"))
    //  We union all edges together to get the full set.
    val allEdges = hvEdges.unionAll(reversedEdges)
    //  Add random parameters from a normal distribution
    val edges = allEdges.withColumn("b", randn(seed + 1) * eStd)  // Ising parameter for edge

    // Create the GraphFrame
    GraphFrame(vertices, edges)
  }

  /** Version of [[gridIsingModel()]] with vStd, eStd set to 1.0. */
  def gridIsingModel(sqlContext: SQLContext, n: Int): GraphFrame =
    gridIsingModel(sqlContext, n, 1.0, 1.0)

}

/** Example GraphFrames for testing the API */
object Graphs extends Graphs
