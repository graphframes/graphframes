package com.databricks.dfgraph.lib

import com.databricks.dfgraph.DFGraph
import org.apache.spark.graphx.{lib => graphxlib}
import org.apache.spark.sql.Row

/**
 * Connected components algorithm.
 */
object ConnectedComponents {

  def run(g: DFGraph): DFGraph = {
    val gx = graphxlib.ConnectedComponents.run(g.cachedGraphX).mapVertices { case (vid, _) => Row(vid) }
    val edgeNames = Seq(DFGraph.ID)
    DFGraph.fromRowGraphX(gx, edgeNames, g.vertices.columns.toSeq)
  }
}
