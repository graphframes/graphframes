package com.databricks.dfgraph.lib

import com.databricks.dfgraph.DFGraph
import com.sun.javaws.exceptions.InvalidArgumentException
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.sql.types.{Metadata, StructField, LongType, StructType}

import org.apache.spark.sql.functions._

/**
 * Convenience functions to map graphX graphs to DFGraphs, checking for the types expected by GraphX.
 */
private[dfgraph] object GraphXConversions {

  import DFGraph._

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
   *    - all the other vertex columns except for the ID column
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
    fromGraphX(originalGraph, gx, originalGraph.edges.schema, vStruct)
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
    // Transform the vertex schema to recover the missing columns:
    //  - the original columns may be present and packed in attr -> we need to unpack them
    //  - the original columns may have been dropped -> we need to join them back
    //  - some extra columns may have been added
    //  - the vertex id may have been translated -> bring it back

    val ee = sqlContext.createDataFrame(edges, edgeSchema)
    // Transform the edge schema:
    //  - unlike vertices, we assume for now the attribute is still here
    //  - the edge src/dst may have been translated -> bring them back
    DFGraph(vv, ee)
  }

  /**
   * Transforms a graphx object encoding dataframe information into a DFGraph, preserving the attributes of the original
   * dataframe in the process.
   *
   * This method is used when a graphx algorithm is called to perform a graph transformation:
   * original (DFGraph) -> graph (graphx) -> new (DFGraph)
   *
   * The following invariants are respected:
   *  - the original attribute columns are carried over
   *  - new data columns added by the graphx transform are added
   *  - the ids, sources and des have the same type in the original and the final graphs
   * @param original
   * @param graph
   * @param edgeSchema the schema for the edges. It is expected to contain an $ID column of type long, and a number of
   *                   extra columns with some data (potentially including a $ATTRS column).
   * @param vertexSchema the schema for the vertices. It is expected to contain the following columns:
   *                      - $SRC of type long
   *                      - $DST of type long
   *                      - a number of extra columns, and potentially a $ATTRS columns
   * @return
   */
  def fromGraphX(
      original: DFGraph,
      graph: Graph[Row, Row],
      edgeSchema: StructType,
      vertexSchema: StructType): DFGraph = {
    val sqlContext = SQLContext.getOrCreate(graph.vertices.context)
    // The dataframe of the gx data:
    val vRows = graph.vertices.map(_._2)
    val gxVertices = sqlContext.createDataFrame(vRows, vertexSchema)
    // TODO(tjh) check the $ID column
    // If necessary, join with the original dataframe to recover the original attributes
    // TODO(tjh) check that the attributes are empty, so that we do not need to do this join?
    val vJoinColName = if (original.indexedVertices.isDefined) { LONG_ID } else { ID }
    val vJoinDF = original.indexedVertices.getOrElse(original.vertices)
    val fullVDF = gxVertices.join(vJoinDF).where(gxVertices(ID) === vJoinDF(vJoinColName))

    // Strip all the extra columns introduced by using surrogate ids.
    // This dataframe contains the ID, all the attributes added by GraphX and all the previous attributes
    val vertexDF = destructAttributes(fullVDF)

    // Edges
    // We assume that the edges have attributes and that we do not need to join against the original graph to
    // recover the attributes.
    val eRows = graph.edges.map(_.attr)
    val gxEdges = sqlContext.createDataFrame(eRows, edgeSchema)
    val edgeDF = destructAttributes(gxEdges)
    DFGraph(vertexDF, edgeDF)
  }

  /**
   * Looks for the $ATTRS columns and deconstructs the structure in this column as top level fields.
   *
   * If the $ATTRS column is missing, or if there in field to destruct, it will return the original dataframe.
   *
   * @param df the dataframe with a column to deconstruct
   * @param skip a set of attributes to skip when deconstructing. They should NOT include the "attrs." suffix.
   * @return
   */
  @throws[InvalidArgumentException]("if argument names are clashing")
  private def destructAttributes(df: DataFrame): DataFrame = {
    // The mapping (in order) of the original columns to the final, flattened columns.
    val cols: Seq[(String, StructField)] = df.schema.fields.flatMap {
      case StructField(name, t: StructType, _, _) if name == ATTR && t.nonEmpty =>
        t.map { f =>
          val newName = f.name.stripPrefix(s"$ATTR.")
          f.name -> f.copy(name = newName)
        }
      case f if f.name == LONG_ID || f.name == ORIGINAL_ID => None
      case f => Some(f.name -> f)
    }
    val selection = cols.map { case (oldName, s) => col(oldName).as(s.name) }
    df.select(selection: _*)
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

private[lib] trait Arguments {
  private[lib] def check[A](a: Option[A], name: String): A = {
    a.getOrElse(throw new IllegalArgumentException)
  }
}
