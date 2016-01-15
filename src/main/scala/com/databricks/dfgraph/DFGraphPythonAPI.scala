package com.databricks.dfgraph

import org.apache.spark.sql.DataFrame


private[dfgraph] class DFGraphPythonAPI {

  def createGraph(v: DataFrame, e: DataFrame) = DFGraph(v, e)

  val ID: String = DFGraph.ID
  val SRC: String = DFGraph.SRC
  val DST: String = DFGraph.DST
  val ATTR: String = DFGraph.ATTR

}
