package com.databricks.dfgraph.lib

import com.databricks.dfgraph.DFGraph

import org.apache.spark.graphx.{Edge, lib => graphxlib}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.reflect._

/**
 * Computes shortest paths to the given set of landmark vertices, returning a graph where each
 * vertex attribute is a map containing the shortest-path distance to each reachable landmark.
 */
object ShortestPaths {

  /**
   * Computes shortest paths to the given set of landmark vertices.
   *
   * @param graph the graph for which to compute the shortest paths
   * @param landmarks the list of landmark vertex ids. Shortest paths will be computed to each
   * landmark.
   *
   * @return a graph where each vertex attribute is a map containing the shortest-path distance to
   * each reachable landmark vertex.
   */
  // TODO(tjh) the vertexId should be checked with an encoder instead
  def run(graph: DFGraph, landmarks: Seq[Long]): DFGraph = {
    val gx = graphxlib.ShortestPaths.run(graph.cachedGraphX, landmarks)
    val rowGx = gx.mapVertices { case (vid, distances) =>
        Row(vid, distances)
    }
    val s = graph.vertices.schema(DFGraph.ID)
    val vStruct = StructType(List(
      s.copy(name = DFGraph.LONG_ID, dataType = LongType),
      StructField(DISTANCE_ID, MapType(s.dataType, IntegerType))))
    GraphXConversions.fromRowGraphX(graph, rowGx, graph.edges.schema, vStruct)
  }

  private val DISTANCE_ID = "distance"

  class Builder private[dfgraph] (graph: DFGraph) extends Arguments {
    private var lmarks: Option[Seq[Long]] = None

    def setLandmarks[VertexType: ClassTag](landmarks: Seq[VertexType]): this.type = {
      val ct = implicitly[ClassTag[VertexType]]
      require(ct == classTag[Long], "Only long are supported for now")
      lmarks = Some(landmarks.map(_.asInstanceOf[Long]))
      this
    }

    def run(): DFGraph = {
      ShortestPaths.run(graph, check(lmarks, "landmarks"))
    }
  }
}
