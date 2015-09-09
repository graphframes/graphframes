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
import org.apache.spark.sql.types.DataType

/**
 * Wraps a [[Graph]] inside a [[DataFrame]]. This makes it easy to implement a Python API,
 * import/export, and other features. The schema of [[vertexDF]] must contain columns with names
 * and types "vtxID" (Long) and "vtxAttr" (VD). The schema of [[edgeDF]] must contain columns
 * with names and types "edgeSrcId" (Long), "edgeTgtId" (Long), "edgeAttr" (ED). Types [[VD]] and
 * [[ED]] must have implicit [[TypeTag]] (required by [[SQLContext.createDataFrame()]] and
 * [[ClassTag]] (required by [[Graph]]) available, and must correspond to a Spark SQL [[DataType]].
 *
 * @param vertexDF the [[DataFrame]] holding vertex information
 * @param edgeDF the [[DataFrame]] holding edge information
 * @tparam VD the type of vertex attributes
 * @tparam ED the type of edge attributes
 */
class DFGraph[VD: TypeTag:ClassTag, ED: TypeTag:ClassTag] protected (
    @transient val vertexDF: DataFrame,
    @transient val edgeDF: DataFrame) extends Serializable {
  import DFGraph._
  private val _sqlContext: SQLContext = vertexDF.sqlContext

  /** Default constructor is provided to support serialization */
  protected def this() = this(null, null)

  /**
   * Converts the data wrapped in [[DataFrame]]s back into a [[Graph]].
   *
   * @return a [[Graph]] with vertices specified by [[vertexDF]] and edges specified by [[edgeDF]]
   */
  def toGraph(): Graph[VD, ED] = {
    val vertices = VertexRDD(vertexDF.select(VERTEX_ID_COLNAME, VERTEX_ATTR_COLNAME).map {
      case Row(id: Long, attr) => (id, attr.asInstanceOf[VD])
    })
    val edges = EdgeRDD.fromEdges[ED, VD](
      edgeDF.select(EDGE_SRC_ID_COLNAME, EDGE_TGT_ID_COLNAME, EDGE_ATTR_COLNAME).map {
        case Row(src: Long, dst: Long, attr) => Edge(src, dst, attr.asInstanceOf[ED])
      }
    )
    Graph(vertices, edges)
  }

  /**
   * Saves the [[DFGraph]] instance to the specified path in Parquet format.
   *
   * @param path the location to write to
   */
  def save(path: String): Unit = DFGraph.save(_sqlContext, vertexDF, edgeDF, path)
}

object DFGraph {
  val VERTEX_ID_COLNAME = "vtxID"
  val VERTEX_ATTR_COLNAME = "vtxAttr"
  val EDGE_SRC_ID_COLNAME = "edgeSrcId"
  val EDGE_TGT_ID_COLNAME = "edgeTgtId"
  val EDGE_ATTR_COLNAME = "edgeAttr"

  private val thisClassName = "com.databricks.dfgraph.DFGraph"
  private val thisFormatVersion = "1.0"
  private val VERTEX_DF_FILENAME = "vtxDF"
  private val EDGE_DF_FILENAME= "edgeDF"

  /**
   * Instantiates a [[DFGraph]] from a [[VertexRDD]] and [[EdgeRDD]].
   */
  def apply[VD: TypeTag:ClassTag, ED: TypeTag:ClassTag](
      vertexRDD: VertexRDD[VD], edgeRDD: EdgeRDD[ED]): DFGraph[VD, ED] = {
    new DFGraph(vertexRDDToDF(vertexRDD), edgeRDDToDF(edgeRDD))
  }

  /**
   * Instantiates a [[DFGraph]] from an existing [[Graph]] instance.
   */
  def apply[VD: TypeTag:ClassTag, ED: TypeTag:ClassTag](graph: Graph[VD, ED]): DFGraph[VD, ED] = {
    this.apply(graph.vertices, graph.edges)
  }

  private def vertexRDDToDF[VD: TypeTag:ClassTag](vtx: VertexRDD[VD]): DataFrame = {
    val sqlContext = SQLContext.getOrCreate(vtx.sparkContext)
    sqlContext.createDataFrame(vtx).toDF(VERTEX_ID_COLNAME, VERTEX_ATTR_COLNAME)
  }

  private def edgeRDDToDF[ED: TypeTag:ClassTag](edge: EdgeRDD[ED]): DataFrame = {
    val sqlContext = SQLContext.getOrCreate(edge.sparkContext)
    sqlContext.createDataFrame(edge)
      .toDF(EDGE_SRC_ID_COLNAME, EDGE_TGT_ID_COLNAME, EDGE_ATTR_COLNAME)
  }

  private def save(sqlContext: SQLContext, vertexDF: DataFrame, edgeDF: DataFrame, path: String): Unit = {
    val metadata = compact(render
      (("class" -> thisClassName) ~ ("version" -> thisFormatVersion)))
    sqlContext.sparkContext.parallelize(Seq(metadata), 1).saveAsTextFile(metadataPath(path))

    val verticesPath = new Path(dataPath(path), VERTEX_DF_FILENAME).toUri.toString
    vertexDF.write.parquet(verticesPath)

    val edgesPath = new Path(dataPath(path), EDGE_DF_FILENAME).toUri.toString
    edgeDF.write.parquet(edgesPath)
  }

  /**
   * Loads and instantiates a [[DFGraph]] previously exported by [[DFGraph.save()]].
   *
   * @param sqlContext the sql context in which to instantiate this [[DFGraph]]'s backing
   *                   [[DataFrame]]s
   * @param path the path to load the data from
   * @tparam VD the vertex attribute type
   * @tparam ED the edge attribute type
   */
  def load[VD:TypeTag:ClassTag, ED:TypeTag:ClassTag](
      sqlContext: SQLContext, path: String): DFGraph[VD, ED] = {
    // TODO: can we store VD, ED in metadata and check for available implicit TypeTags on load?
    val (loadedClassName, loadedVersion, _) = loadMetadata(sqlContext.sparkContext, path)
    assert(thisClassName == loadedClassName,
      s"DFGraph.load expected className to be $thisClassName but got $loadedClassName")
    assert(thisFormatVersion == loadedVersion,
      s"DFGraph.load expected format verion to be $thisFormatVersion but got $loadedVersion")

    val vertexDataPath = new Path(dataPath(path), VERTEX_DF_FILENAME).toUri.toString
    val edgeDataPath = new Path(dataPath(path), EDGE_DF_FILENAME).toUri.toString

    val vtxDF = sqlContext.read.parquet(vertexDataPath)
    val edgeDF = sqlContext.read.parquet(edgeDataPath)
    new DFGraph(vtxDF, edgeDF)
  }

  // TODO: reuse mllib.util.modelSaveLoad
  private def metadataPath(path: String): String = new Path(path, "metadata").toUri.toString

  private def dataPath(path: String): String = new Path(path, "data").toUri.toString

  private def loadMetadata(sc: SparkContext, path: String): (String, String, JValue) = {
    implicit val formats = DefaultFormats
    val metadata = parse(sc.textFile(metadataPath(path)).first())
    val clazz = (metadata \ "class").extract[String]
    val version = (metadata \ "version").extract[String]
    (clazz, version, metadata)
  }
}
