package com.databricks.dfgraph.lib

import com.databricks.dfgraph.DFGraph
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.types.{Metadata, StructField, LongType, StructType}

/**
 * Convenience functions to map graphX graphs to DFGraphs, checking for the types expected by GraphX.
 */
private[dfgraph] object GraphXConversions {


  /**
   * Takes a graph built through some graphX algorithm by transforming 'originalGraph',
   * and converts it back to a DFGraph.
   *
   * It is assumed the graph returned by graphX only stores a vertex id as the payload for the
   * vertices.
   *
   * The new graph has the following schema:
   *  - the edges have the same schema (but they may be a subset only of the original vertices)
   *  - the vertices have the following columns:
   *    - ID: same type and metadata as the original column (coerced to be Long because of GraphX restrictions)
   *    - `vertexColumnName` with type Long
   *
   * @param graph the graph
   * @return
   */
  def fromVertexGraphX(
      graph: Graph[VertexId, Row],
      originalGraph: DFGraph,
      vertexColumnName: String): DFGraph = {
    val gx = graph.mapVertices { case (vid, vlabel) =>
      Row(vid, vlabel)
    }
    val s = originalGraph.vertices.schema(DFGraph.ID)
    val vStruct = StructType(List(
      s,
      s.copy(name = vertexColumnName, metadata = Metadata.empty)))
    fromRowGraphX(gx, originalGraph.edges.schema, vStruct)
  }

  /**
   * Converts the dataframes (encoded as RDDs of Row objects) and transforms them back into an [[DFGraph]], preserving
   * all the metadata attributes in the process.
   *
   * This is an internal method to act as a bridge between graphX and non-native DFGraph algorithms.
   *
   * It is assumed that the Row objects contain the necessary attributes (id, src, dst, etc.), as they are dropped from
   * the VertexRDD and the EdgeRDD.
t   *
   * @param graph the graph to convert
   * @param edgeSchema the edge schema
   * @param vertexSchema the vertex schema
   * @return an equivalent [[DFGraph]] object
   */
  def fromRowGraphX(
      graph: Graph[Row, Row],
      edgeSchema: StructType,
      vertexSchema: StructType): DFGraph = {
    val sqlContext = SQLContext.getOrCreate(graph.vertices.context)
    val vertices = graph.vertices.map(_._2)
    val edges = graph.edges.map(_.attr)
    val vv = sqlContext.createDataFrame(vertices, vertexSchema)
    val ee = sqlContext.createDataFrame(edges, edgeSchema)
    DFGraph(vv, ee)
  }


  @throws[IllegalArgumentException]("When the vertex id type is not a long")
  def checkVertexId(graph: DFGraph): Unit = {
    val tpe = graph.vertices.schema(DFGraph.ID).dataType
    if (tpe != LongType) {
      throw new IllegalArgumentException(
        s"Vertex column ${DFGraph.ID} has type $tpe. This type is not supported for this algorithm. " +
        s"Use type Long instead for this column")
    }
  }

}
