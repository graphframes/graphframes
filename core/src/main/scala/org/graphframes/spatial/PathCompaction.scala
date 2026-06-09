package org.graphframes.spatial

import org.graphframes.GraphFrame
import org.graphframes.Logging
import org.graphframes.WithIntermediateStorageLevel
import org.graphframes.WithCheckpointInterval
import scala.collection.mutable
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.*

class PathCompaction private[graphframes] (graph: GraphFrame)
    extends Logging
    with WithIntermediateStorageLevel
    with WithCheckpointInterval {

  private var dropCompactedCycles: Boolean = true
  private val aggExpressions: mutable.Map[String, Column] = mutable.Map.empty
  private var alwaysKeepVertices: Option[Column] = None
  private var alwaysDropVertices: Option[Column] = None
  private var clusteringAlgorithm: String = "two_phase"

  private var requiredVertexAttributes: Seq[String] =
    graph.vertices.columns.filter(c => c != GraphFrame.ID)

  def setDropCompactedCycles(value: Boolean): this.type = {
    dropCompactedCycles = value
    this
  }

  def withAggExpression(name: String, expr: Column): this.type = {
    val _ = aggExpressions.put(name, expr)
    this
  }

  def setAlwaysKeepVertices(value: Column): this.type = {
    alwaysKeepVertices = Some(value)
    this
  }

  def setAlwaysDropVertices(value: Column): this.type = {
    alwaysDropVertices = Some(value)
    this
  }

  def setClusteringAlgorithm(value: String): this.type = {
    require(
      PathCompaction.supportedAlgorithms.contains(value),
      s"algorithm $value is not supported; supported algorithms: ${PathCompaction.supportedAlogrithmsAsString}")
    clusteringAlgorithm = value
    this
  }

  def run(): DataFrame = {
    val subgraph = alwaysDropVertices match {
      case Some(filter) => graph.filterVertices(!filter)
      case None => graph
    }

    val candidates = {
      val degreesFilter = subgraph.inDegrees
        .join(subgraph.outDegrees, Seq(GraphFrame.ID), "inner")
        .filter((col("inDegree") === lit(1)) && (col("outDegree") === lit(1)))
      if (alwaysKeepVertices.isEmpty) {
        degreesFilter.select(GraphFrame.ID)
      } else {
        degreesFilter
          .join(subgraph.vertices.filter(alwaysKeepVertices.get), Seq(GraphFrame.ID), "left_anti")
          .select(GraphFrame.ID)
      }
    }

    null
  }
}

object PathCompaction extends Serializable {
  // It would be nice to add in the future a specialized version based on pointers jump
  private val supportedAlgorithms = Seq("two_phase", "randomized_contraction")
  private def supportedAlogrithmsAsString = supportedAlgorithms.mkString(", ")
}
