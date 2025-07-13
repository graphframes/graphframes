package org.graphframes.propertygraph.property

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.sha2
import org.apache.spark.sql.types._
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
 *   - Source and destination vertex property groups
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
 * @param srcPropertyGroup
 *   Source vertex property group
 * @param dstPropertyGroup
 *   Destination vertex property group
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
case class EdgePropertyGroup private (
    name: String,
    data: DataFrame,
    srcPropertyGroup: VertexPropertyGroup,
    dstPropertyGroup: VertexPropertyGroup,
    isDirected: Boolean,
    srcColumnName: String,
    dstColumnName: String,
    weightColumnName: String)
    extends PropertyGroup {

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
    val weightColumnType = data.schema(weightColumnName).dataType
    if (!weightColumnType.isInstanceOf[NumericType]) {
      throw new InvalidPropertyGroupException(
        s"weight column $weightColumnName must be numeric type, but was $weightColumnType")
    }

    this
  }

  private def hashSrcEdge: Column =
    concat(lit(srcPropertyGroup.name), sha2(col(srcColumnName).cast("string"), 256))
  private def hashDstEdge: Column =
    concat(lit(dstPropertyGroup.name), sha2(col(dstColumnName).cast("string"), 256))

  override protected[graphframes] def getData(filter: Column): DataFrame = {
    val filteredData = data.filter(filter)

    val baseEdges = filteredData.select(
      hashSrcEdge.alias(GraphFrame.SRC),
      hashDstEdge.alias(GraphFrame.DST),
      col(weightColumnName).alias(GraphFrame.WEIGHT))

    if (isDirected) {
      baseEdges
    } else {
      baseEdges.union(
        baseEdges.select(
          col(GraphFrame.DST).as(GraphFrame.SRC),
          col(GraphFrame.SRC).as(GraphFrame.DST),
          col(GraphFrame.WEIGHT).alias(GraphFrame.WEIGHT)))
    }
  }
}

object EdgePropertyGroup {
  def apply(
      name: String,
      data: DataFrame,
      srcPropertyGroup: VertexPropertyGroup,
      dstPropertyGroup: VertexPropertyGroup,
      isDirected: Boolean,
      srcColumnName: String,
      dstColumnName: String,
      weightColumnName: String): EdgePropertyGroup = {
    new EdgePropertyGroup(
      name,
      data,
      srcPropertyGroup,
      dstPropertyGroup,
      isDirected,
      srcColumnName,
      dstColumnName,
      weightColumnName).validate()
  }

  def apply(
      name: String,
      data: DataFrame,
      srcPropertyGroup: VertexPropertyGroup,
      dstPropertyGroup: VertexPropertyGroup,
      isDirected: Boolean,
      srcColumnName: String,
      dstColumnName: String,
      weightColumn: Column): EdgePropertyGroup = {
    val dataWithWeight = data.withColumn(GraphFrame.WEIGHT, weightColumn)
    apply(
      name,
      dataWithWeight,
      srcPropertyGroup,
      dstPropertyGroup,
      isDirected,
      srcColumnName,
      dstColumnName,
      GraphFrame.WEIGHT)
  }
}
