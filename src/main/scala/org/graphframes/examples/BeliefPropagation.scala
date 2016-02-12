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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge => GXEdge,VertexRDD, Graph}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions.{col, udf, randn}

import org.graphframes.GraphFrame


/**
 * Example code for Belief Propagation (BP)
 *
 * This provides a template for building customized BP algorithms for different types of
 * graphical models.
 *
 * This example:
 *  - Ising model on a grid
 *  - Parallel Belief Propagation using colored fields
 *
 * Ising models are probabilistic graphical models over binary variables x,,i,,.
 * Each binary variable x,,i,, corresponds to one vertex, and it may take values -1 or +1.
 * The probability distribution P(X) (over all x,,i,,) is parameterized by vertex factors a,,i,,
 * and edge factors b,,ij,,:
 * {{{
 *  P(X) = (1/Z) * exp[ \sum_i a_i x_i + \sum_{ij} b_{ij} x_i x_j ]
 * }}}
 * where Z is the normalization constant (partition function).
 * See [[https://en.wikipedia.org/wiki/Ising_model Wikipedia]] for more information on Ising models.
 *
 * Belief Propagation (BP) provides marginal probabilities of the values of the variables x,,i,,,
 * i.e., P(x,,i,,) for each i.  This allows a user to understand likely values of variables.
 * See [[https://en.wikipedia.org/wiki/Belief_propagation Wikipedia]] for more information on BP.
 *
 * We use a batch synchronous BP algorithm, where batches of vertices are updated synchronously.
 * We follow the mean field update algorithm in Slide 13 of the
 * [[http://www.eecs.berkeley.edu/~wainwrig/Talks/A_GraphModel_Tutorial  talk slides]] from:
 *  Wainwright. "Graphical models, message-passing algorithms, and convex optimization."
 *
 * The batches are chosen according to a coloring.  For background on graph colorings for inference,
 * see for example:
 *  Gonzalez et al. "Parallel Gibbs Sampling: From Colored Fields to Thin Junction Trees."
 *  AISTATS, 2011.
 *
 * The BP algorithm works by:
 *  - Coloring the graph by assigning a color to each vertex such that no neighboring vertices
 *    share the same color.
 *  - In each step of BP, update all vertices of a single color.  Alternate colors.
 */
object BeliefPropagation {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BeliefPropagation example")
    val sc = SparkContext.getOrCreate(conf)
    val sql = SQLContext.getOrCreate(sc)

    // Create graphical model g.
    val n = 3
    val g = createGridGraph(sql, n)

    // Run BP for 5 iterations.
    val results = runBP(g, 5)

    // Display beliefs.
    val beliefs = results.vertices.select("id", "belief")
    beliefs.show()

    sc.stop()
  }

  /**
   * This method generates a grid Ising model with random parameters.
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
  def createGridGraph(sqlContext: SQLContext, n: Int, vStd: Double, eStd: Double): GraphFrame = {
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

  /** Version of [[createGridGraph()]] with vStd, eStd set to 1.0. */
  def createGridGraph(sqlContext: SQLContext, n: Int): GraphFrame =
    createGridGraph(sqlContext, n, 1.0, 1.0)

  /**
   * Given a GraphFrame, choose colors for each vertex.  No neighboring vertices will share the
   * same color.  The number of colors is minimized.
   *
   * This is written specifically for grid graphs. For non-grid graphs, it should be generalized,
   * such as by using a greedy coloring scheme.
   *
   * @param g  Grid graph generated by [[createGridGraph()]]
   * @return  Same graph, but with a new vertex column "color" of type Int (0 or 1)
   */
  private def colorGraph(g: GraphFrame): GraphFrame = {
    val colorUDF = udf { (i: Int, j: Int) => if ((i + j) % 2 == 0) 0 else 1 }
    val v = g.vertices.withColumn("color", colorUDF(col("i"), col("j")))
    GraphFrame(v, g.edges)
  }

  /**
   * Run Belief Propagation.
   *
   * This implementation of BP shows how to use GraphX's aggregationMessages method.
   * It is simple to convert to and from GraphX format.  This method does the following:
   *  - Color GraphFrame vertices for BP scheduling.
   *  - Convert GraphFrame to GraphX format.
   *  - Run BP using GraphX's aggregateMessages API.
   *  - Augment the original GraphFrame with the BP results (vertex beliefs).
   *
   * @param g  Graphical model created by [[createGridGraph()]]
   * @param numIter  Number of iterations of BP to run.  One iteration includes updating each
   *                 vertex's belief once.
   * @return  Same graphical model, but with [[GraphFrame.vertices]] augmented with a new column
   *          "belief" containing P(x,,i,, = +1), the marginal probability of vertex i taking
   *          value +1 instead of -1.
   */
  def runBP(g: GraphFrame, numIter: Int): GraphFrame = {
    // Choose colors for vertices for BP scheduling.
    val colorG = colorGraph(g)
    val numColors: Int = colorG.vertices.select("color").distinct.count().toInt

    // Convert GraphFrame to GraphX, and initialize beliefs.
    val gx0 = colorG.toGraphX
    // Schema maps for extracting attributes
    val vColsMap = colorG.vColsMap
    val eColsMap = colorG.eColsMap
    // Convert vertex attributes to nice case classes.
    val gx1: Graph[VertexAttr, Row] = gx0.mapVertices { case (_, attr) =>
      // Initialize belief at 0.0
      VertexAttr(attr.getDouble(vColsMap("a")), 0.0, attr.getInt(vColsMap("color")))
    }
    // Convert edge attributes to nice case classes.
    val extractEdgeAttr: (GXEdge[Row] => EdgeAttr) = { e =>
      // Initialize belief at 0.0
      EdgeAttr(e.attr.getDouble(eColsMap("b")), 0.0)
    }
    var gx: Graph[VertexAttr, EdgeAttr] = gx1.mapEdges(extractEdgeAttr)

    // Run BP for numIter iterations.
    for (iter <- Range(0, numIter)) {
      // For each color, have that color receive messages from neighbors.
      for (color <- Range(0, numColors)) {
        // Send messages to vertices of the current color.
        val msgs: VertexRDD[Double] = gx.aggregateMessages(
          ctx => if (ctx.dstAttr.color == color) {
            val msg = ctx.attr.b * ctx.srcAttr.belief
            // Only send message if non-zero.
            if (msg != 0) ctx.sendToDst(msg)
          },
          _ + _)
        // Receive messages, and update beliefs for vertices of the current color.
        gx = gx.outerJoinVertices(msgs) {
          case (vID, vAttr, optMsg) =>
            if (vAttr.color == color) {
              val x = vAttr.a + optMsg.getOrElse(0.0)
              val newBelief = math.exp(-log1pExp(-x))
              VertexAttr(vAttr.a, newBelief, color)
            } else {
              vAttr
            }
        }
      }
    }

    // Convert back to GraphFrame with a new column "belief" for vertices DataFrame.
    val gxFinal: Graph[Double, Unit] = gx.mapVertices((_, attr) => attr.belief).mapEdges(_ => ())
    GraphFrame.fromGraphX(g, gxFinal, vertexNames = Seq("belief"))
  }

  case class VertexAttr(a: Double, belief: Double, color: Int)

  case class EdgeAttr(b: Double, belief: Double)

  /** More numerically stable `log(1 + exp(x))` */
  private def log1pExp(x: Double): Double = {
    if (x > 0) {
      x + math.log1p(math.exp(-x))
    } else {
      math.log1p(math.exp(x))
    }
  }
}
