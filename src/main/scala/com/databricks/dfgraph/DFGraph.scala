package com.databricks.dfgraph

import org.apache.spark.graphx.{Edge, Graph, EdgeRDD, VertexRDD}
import org.apache.spark.sql.{Row, SQLContext, DataFrame}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
 * Wraps a GraphX graph inside a Spark Dataframe.
 * @tparam VD vertex attribute type
 */
class DFGraph[VD: TypeTag:ClassTag, ED: TypeTag:ClassTag](
    private val vertexDF: DataFrame,
    private val edgeDF: DataFrame) extends Serializable {
  import DFGraph._
  private val _sqlContext: SQLContext = vertexDF.sqlContext

  def toGraph(): Graph[VD, ED] = {
    val verts = VertexRDD(vertexDF.select(VERTEX_ID_COLNAME, VERTEX_ATTR_COLNAME).map {
      case Row(id: Long, attr: VD) => (id, attr)
    })
    edgeDF.select(EDGE_SRC_ID_COLNAME, EDGE_TGT_ID_COLNAME, EDGE_ATTR_COLNAME).printSchema()
    edgeDF.select(EDGE_SRC_ID_COLNAME, EDGE_TGT_ID_COLNAME, EDGE_ATTR_COLNAME).show()
    val edges = EdgeRDD.fromEdges[ED, VD](
      edgeDF.select(EDGE_SRC_ID_COLNAME, EDGE_TGT_ID_COLNAME, EDGE_ATTR_COLNAME).map {
        // TODO: fix matcherror
        case Row(src: Long, dst: Long, attr: ED) => Edge(src, dst, attr)
      }
    )
    Graph(verts, edges)
  }
}

private object DFGraph {
  val VERTEX_ID_COLNAME = "vtxID"
  val VERTEX_ATTR_COLNAME = "vtxAttr"
  val EDGE_SRC_ID_COLNAME = "edgeSrcId"
  val EDGE_TGT_ID_COLNAME = "edgeTgtId"
  val EDGE_ATTR_COLNAME = "edgeAttr"

  def vertexRDDToDf[VD: TypeTag:ClassTag](vtx: VertexRDD[VD]): DataFrame = {
    val sqlContext = SQLContext.getOrCreate(vtx.sparkContext)
    sqlContext.createDataFrame(vtx).toDF(VERTEX_ID_COLNAME, VERTEX_ATTR_COLNAME)
  }

  def edgeRDDToDf[ED: TypeTag:ClassTag](edge: EdgeRDD[ED]): DataFrame = {
    val sqlContext = SQLContext.getOrCreate(edge.sparkContext)
    sqlContext.createDataFrame(edge)
      .toDF(EDGE_SRC_ID_COLNAME, EDGE_TGT_ID_COLNAME, EDGE_ATTR_COLNAME)
  }

  def apply[VD: TypeTag:ClassTag, ED: TypeTag:ClassTag](vertexRDD: VertexRDD[VD], edgeRDD: EdgeRDD[ED]) = {
    new DFGraph[VD, ED](vertexRDDToDf(vertexRDD), edgeRDDToDf(edgeRDD))
  }
}
