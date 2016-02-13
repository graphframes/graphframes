package org.graphframes

import org.apache.spark.sql.DataFrame

import org.graphframes.examples.Graphs

private[graphframes] class GraphFramePythonAPI {

  def createGraph(v: DataFrame, e: DataFrame) = GraphFrame(v, e)

  val ID: String = GraphFrame.ID
  val SRC: String = GraphFrame.SRC
  val DST: String = GraphFrame.DST
  val ATTR: String = GraphFrame.ATTR

  lazy val examples: Graphs = Graphs
}
