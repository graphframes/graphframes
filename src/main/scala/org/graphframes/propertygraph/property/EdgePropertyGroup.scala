package org.graphframes.propertygraph.property

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.sha2
import org.graphframes.GraphFrame
import org.graphframes.InvalidPropertyGroupException

/**
 * Represents a logical group of edges in a property graph with associated metadata and data.
 *
 * EdgePropertyGroup encapsulates edge data stored in a DataFrame along with metadata describing
 * how to interpret the data as graph edges. Each edge group has:
 *
 *   - A unique name identifier
 *   - DataFrame containing the actual edge data
 *   - Names of source and destination vertex property groups
 *   - Direction flag indicating if edges are directed or undirected
 *   - Column names specifying source vertex, destination vertex and edge weight columns
 *
 * The class validates that required columns exist in the provided DataFrame on creation. Required
 * columns are:
 *   - Source vertex column
 *   - Destination vertex column
 *   - Weight column
 *
 * @param name
 *   Unique identifier for this edge property group
 * @param data
 *   DataFrame containing the edge data with required columns
 * @param srcPropertyGroupName
 *   Name of the source vertex property group
 * @param dstPropertyGroupName
 *   Name of the destination vertex property group
 * @param isDirected
 *   Whether edges should be treated as directed (true) or undirected (false)
 * @param srcColumnName
 *   Name of the source vertex column in the data
 * @param dstColumnName
 *   Name of the destination vertex column in the data
 * @param weightColumnName
 *   Name of the edge weight column in the data
 * @note
 *   When edges from different groups are combined into a GraphFrame, their SRCs and DSTs are
 *   hashed with the group name to prevent collisions in the same way as ID of the corresponded
 *   vertex group is hashed.
 */
case class EdgePropertyGroup(
    val name: String,
    val data: DataFrame,
    srcPropertyGroupName: String,
    dstPropertyGroupName: String,
    isDirected: Boolean,
    srcColumnName: String,
    dstColumnName: String,
    weightColumnName: String)
    extends PropertyGroup {
  import EdgePropertyGroup._

  override protected def validate(): this.type = {
    if (!data.columns.contains(srcColumnName)) {
      throw new InvalidPropertyGroupException(
        s"source column $srcColumnName does not exist, existed columns [${data.columns.mkString(", ")}]")
    }
    if (!data.columns.contains(dstColumnName)) {
      throw new InvalidPropertyGroupException(
        s"dest column $dstColumnName does not exist, existed columns [${data.columns.mkString(", ")}]")
    }
    if (!data.columns.contains(weightColumnName)) {
      throw new InvalidPropertyGroupException(
        s"weight column $weightColumnName does not exist, existed columns [${data.columns.mkString(", ")}]")
    }
    this
  }

  private val hashSrcEdge: Column =
    concat(lit(srcPropertyGroupName), sha2(col(srcColumnName), 256))
  private val hashDstEdge: Column =
    concat(lit(dstPropertyGroupName), sha2(col(dstColumnName), 256))

  override protected[graphframes] def internalIdMapping: DataFrame = {
    data
      .select(col(srcColumnName))
      .distinct()
      .select(col(srcColumnName).alias(EXTERNAL_ID), hashSrcEdge.alias(INTERNAL_ID))
      .union(
        data
          .select(col(dstColumnName))
          .distinct()
          .select(col(dstColumnName).alias(EXTERNAL_ID), hashDstEdge.alias(INTERNAL_ID)))
      .distinct()
  }

  override protected[graphframes] def getData(filter: Column): DataFrame = {
    val filteredData = data.filter(filter)

    val baseEdges = filteredData.select(
      hashSrcEdge.alias(GraphFrame.SRC),
      hashDstEdge.alias(GraphFrame.DST),
      col(weightColumnName))

    if (isDirected) {
      baseEdges
    } else {
      baseEdges.union(
        baseEdges.select(
          col(GraphFrame.DST).as(GraphFrame.SRC),
          col(GraphFrame.SRC).as(GraphFrame.DST),
          col(weightColumnName)))
    }
  }
}

object EdgePropertyGroup {
  private val EXTERNAL_ID = "externalId"
  private val INTERNAL_ID = "internalId"

  def apply(
      name: String,
      data: DataFrame,
      srcPropertyGroup: VertexPropertyGroup,
      dstPropertyGroup: VertexPropertyGroup,
      isDirected: Boolean,
      srcColumnName: String,
      dstColumnName: String,
      weightColumnName: String): EdgePropertyGroup = {
    EdgePropertyGroup(
      name,
      data,
      srcPropertyGroup.name,
      dstPropertyGroup.name,
      isDirected,
      srcColumnName,
      dstColumnName,
      weightColumnName)
  }
}
