package com.databricks.dfgraph

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import org.apache.hadoop.fs.Path
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeRDD, Graph, VertexRDD}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
 * Wraps a GraphX graph inside a Spark Dataframe.
 * @tparam VD vertex attribute type
 * @tparam ED edge attribute type
 */
class DFGraph[VD: TypeTag:ClassTag, ED: TypeTag:ClassTag] private (
    val vertexDF: DataFrame,
    val edgeDF: DataFrame) extends Serializable {
  import DFGraph._
  private val _sqlContext: SQLContext = vertexDF.sqlContext

  def toGraph(): Graph[VD, ED] = {
    val verts = VertexRDD(vertexDF.select(VERTEX_ID_COLNAME, VERTEX_ATTR_COLNAME).map {
      case Row(id: Long, attr) => (id, attr.asInstanceOf[VD])
    })

    edgeDF.select(EDGE_SRC_ID_COLNAME, EDGE_TGT_ID_COLNAME, EDGE_ATTR_COLNAME).printSchema()
    val edges = EdgeRDD.fromEdges[ED, VD](
      edgeDF.select(EDGE_SRC_ID_COLNAME, EDGE_TGT_ID_COLNAME, EDGE_ATTR_COLNAME).map {
        case Row(src: Long, dst: Long, attr) => Edge(src, dst, attr.asInstanceOf[ED])
      }
    )
    Graph(verts, edges)
  }

  def save(path: String): Unit = DFGraph.save(_sqlContext, vertexDF, edgeDF, path)
}

object DFGraph {
  private val VERTEX_ID_COLNAME = "vtxID"
  private val VERTEX_ATTR_COLNAME = "vtxAttr"
  private val EDGE_SRC_ID_COLNAME = "edgeSrcId"
  private val EDGE_TGT_ID_COLNAME = "edgeTgtId"
  private val EDGE_ATTR_COLNAME = "edgeAttr"

  val thisFormatVersion = "1.0"

  val thisClassName = "com.databricks.dfgraph.DFGraph"

  def apply[VD: TypeTag:ClassTag, ED: TypeTag:ClassTag](
      vertexRDD: VertexRDD[VD], edgeRDD: EdgeRDD[ED]): DFGraph[VD, ED] = {
    new DFGraph[VD, ED](vertexRDDToDf(vertexRDD), edgeRDDToDf(edgeRDD))
  }

  def apply[VD: TypeTag:ClassTag, ED: TypeTag:ClassTag](graph: Graph[VD, ED]): DFGraph[VD, ED] = {
    this.apply(graph.vertices, graph.edges)
  }

  private def vertexRDDToDf[VD: TypeTag:ClassTag](vtx: VertexRDD[VD]): DataFrame = {
    val sqlContext = SQLContext.getOrCreate(vtx.sparkContext)
    sqlContext.createDataFrame(vtx).toDF(VERTEX_ID_COLNAME, VERTEX_ATTR_COLNAME)
  }

  private def edgeRDDToDf[ED: TypeTag:ClassTag](edge: EdgeRDD[ED]): DataFrame = {
    val sqlContext = SQLContext.getOrCreate(edge.sparkContext)
    sqlContext.createDataFrame(edge)
      .toDF(EDGE_SRC_ID_COLNAME, EDGE_TGT_ID_COLNAME, EDGE_ATTR_COLNAME)
  }

  def save(sqlContext: SQLContext, vertexDF: DataFrame, edgeDF: DataFrame, path: String): Unit = {
    val metadata = compact(render
      (("class" -> thisClassName) ~ ("version" -> thisFormatVersion)))
    sqlContext.sparkContext.parallelize(Seq(metadata), 1).saveAsTextFile(metadataPath(path))

    val verticesPath = new Path(dataPath(path), "vtxDF").toUri.toString
    vertexDF.write.parquet(verticesPath)

    val edgesPath = new Path(dataPath(path), "edgeDF").toUri.toString
    edgeDF.write.parquet(edgesPath)
  }

  // TODO: store VD, ED in metadata and automatically provide
  def load[VD:TypeTag:ClassTag, ED:TypeTag:ClassTag](
      sqlContext: SQLContext, path: String): DFGraph[VD, ED] = {
    val (loadedClassName, loadedVersion, _) = loadMetadata(sqlContext.sparkContext, path)

    val vertexDataPath = new Path(dataPath(path), "vtxDF").toUri.toString
    val edgeDataPath = new Path(dataPath(path), "edgeDF").toUri.toString

    val vtxDF = sqlContext.read.parquet(vertexDataPath)
    val edgeDF = sqlContext.read.parquet(edgeDataPath)
    new DFGraph[VD, ED](vtxDF, edgeDF)
  }

  // TODO: reuse mllib.util.modelSaveLoad
  private def metadataPath(path: String): String = new Path(path, "metadata").toUri.toString
  private def dataPath(path: String): String = new Path(path, "data").toUri.toString

  /**
   * Load metadata from the given path.
   * @return (class name, version, metadata)
   */
  private def loadMetadata(sc: SparkContext, path: String): (String, String, JValue) = {
    implicit val formats = DefaultFormats
    val metadata = parse(sc.textFile(metadataPath(path)).first())
    val clazz = (metadata \ "class").extract[String]
    val version = (metadata \ "version").extract[String]
    (clazz, version, metadata)
  }
}
