package org.graphframes

import org.apache.spark.sql.DataFrame
import org.graphframes.examples.Graphs
import org.graphframes.lib.AggregateMessages

private[graphframes] class GraphFramePythonAPI {

  def createGraph(v: DataFrame, e: DataFrame): GraphFrame = GraphFrame(v, e)

  val ID: String = GraphFrame.ID
  val SRC: String = GraphFrame.SRC
  val DST: String = GraphFrame.DST
  val EDGE: String = GraphFrame.EDGE
  val ATTR: String = GraphFrame.ATTR

  lazy val aggregateMessages: AggregateMessages.type = AggregateMessages
  lazy val examples: Graphs = Graphs
}
