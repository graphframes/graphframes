package org.graphframes

import org.apache.spark.sql.{SQLContext, DataFrame}


private[graphframes] class GraphFramePythonAPI {

  def createGraph(v: DataFrame, e: DataFrame) = GraphFrame(v, e)

  val ID: String = GraphFrame.ID
  val SRC: String = GraphFrame.SRC
  val DST: String = GraphFrame.DST
  val ATTR: String = GraphFrame.ATTR

  lazy val examples: ExampleImpl = new ExampleImpl
}

/**
 * Some standard structures in the graph literature.
 *
 * These are useful for running tests.
 */
private[graphframes] class ExampleImpl {

  /**
   * Two densely connected blobs (vertices 0->n-1 and n->2n-1) connected by a single edge (0->n)
   * @param blobSize the size of each blob.
   * @return
   */
  def twoBlobs(sqlContext: SQLContext, blobSize: Int): GraphFrame = {
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
   * @param sqlContext
   * @param n the number of leaves
   * @return
   */
  def star(sqlContext: SQLContext, n: Int): GraphFrame = {
    val vertices = sqlContext.createDataFrame(Seq((0, "root")) ++ (1 to n).map { i =>
      (i, s"node-$i")
    }).toDF("id", "v_attr1")
    val edges = sqlContext.createDataFrame((1 to n).map { i =>
      (i, 0, s"edge-$i")
    }).toDF("src", "dst", "e_attr1")
    GraphFrame(vertices, edges)
  }

}

private[graphframes] object Examples extends ExampleImpl
