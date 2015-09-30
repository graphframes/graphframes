package com.databricks.dfgraph

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

/**
 * Represents a [[Graph]] with vertices and edges stored as [[DataFrame]]s.
 * [[vertices]] must contain a column named "id" that stores unique vertex IDs (long type).
 * [[edges]] must contain two columns "src_id" and "dst_id" that store source vertex IDs and target
 * vertex IDs of edges, respectively.
 *
 * @param vertices the [[DataFrame]] holding vertex information
 * @param edges the [[DataFrame]] holding edge information
 */
class DFGraph(
    @transient val vertices: DataFrame,
    @transient val edges: DataFrame) extends Serializable {

  import DFGraph._

  require(vertices.columns.contains(ID) && vertices.schema(ID).dataType == LongType)
  require(edges.columns.contains(SRC_ID) && edges.schema(SRC_ID).dataType == LongType &&
    edges.columns.contains(DST_ID) && edges.schema(SRC_ID).dataType == LongType)

  def sqlContext: SQLContext = vertices.sqlContext

  /**
   * Converts this [[DFGraph]] instance to a GraphX [[Graph]].
   * Vertex and edge attributes are the original rows in [[vertices]] and [[edges]], respectively.
   */
  def toGraphX: Graph[Row, Row] = {
    val vv = vertices.select(col(ID), struct("*")).map { case Row(id: Long, attr: Row) =>
      (id, attr)
    }
    val ee = edges.select(col(SRC_ID), col(DST_ID), struct("*")).map {
      case Row(srcId: Long, dstId: Long, attr: Row) =>
        Edge(srcId, dstId, attr)
    }
    Graph(vv, ee)
  }

  /**
   * Saves the [[DFGraph]] instance to the specified path in Parquet format.
   */
  def save(path: String): Unit = {
    vertices.write.parquet(path + "/" + VERTICES)
    edges.write.parquet(path + "/" + EDGES)
  }
}

object DFGraph {

  val ID: String = "id"
  val ATTR: String = "attr"
  val SRC_ID: String = "src_id"
  val DST_ID: String = "dst_id"
  val VERTICES: String = "vertices"
  val EDGES: String = "edges"

  /**
   * Instantiates a [[DFGraph]] from an existing [[Graph]] instance.
   */
  def fromGraphX[VD : TypeTag, ED : TypeTag](graph: Graph[VD, ED]): DFGraph = {
    val sqlContext = SQLContext.getOrCreate(graph.vertices.context)
    val vv = sqlContext.createDataFrame(graph.vertices).toDF(ID, ATTR)
    val ee = sqlContext.createDataFrame(graph.edges).toDF(SRC_ID, DST_ID, ATTR)
    new DFGraph(vv, ee)
  }

  /**
   * Loads and instantiates a [[DFGraph]] previously exported by [[DFGraph.save()]].
   *
   * @param sqlContext the sql context in which to instantiate this [[DFGraph]]'s backing
   *                   [[DataFrame]]s
   * @param path the path to load the data from
   */
  def load(sqlContext: SQLContext, path: String): DFGraph = {
    val vertices = sqlContext.read.parquet(path + "/" + VERTICES)
    val edges = sqlContext.read.parquet(path + "/" + EDGES)
    new DFGraph(vertices, edges)
  }
}
