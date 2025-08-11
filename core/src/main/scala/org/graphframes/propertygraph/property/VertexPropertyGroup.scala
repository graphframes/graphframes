package org.graphframes.propertygraph.property

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.sha2
import org.apache.spark.sql.types.StringType
import org.graphframes.GraphFrame
import org.graphframes.InvalidPropertyGroupException
import org.graphframes.propertygraph.PropertyGraphFrame

/**
 * Represents a logical group of vertices in a property graph with associated data and
 * identification.
 *
 * A VertexPropertyGroup is used to organize and manage vertices that share common characteristics
 * or belong to the same logical group within a property graph. Each group maintains its own data
 * in the form of a DataFrame and uses a primary key column for unique vertex identification.
 *
 * The class provides two ways to create a vertex property group:
 *   1. With a specified primary key column:
 *      {{{
 *    VertexPropertyGroup("users", userDataFrame, "userId")
 *      }}}
 *   2. With the default primary key column ("id"):
 *      {{{
 *    VertexPropertyGroup("users", userDataFrame)
 *      }}}
 *
 * @param name
 *   The unique identifier for this vertex property group
 * @param data
 *   The DataFrame containing the vertex data
 * @param primaryKeyColumn
 *   The column name used to uniquely identify vertices in this group
 * @param applyMaskOnId
 *   A flag indicating whether to apply masking on vertex IDs. When false, uses raw IDs from
 *   primaryKeyColumn. When true, hashes IDs with group name. Defaults to true.
 * @note
 *   When vertices from different groups are combined into a GraphFrame, their IDs are hashed with
 *   the group name to prevent collisions.
 */
case class VertexPropertyGroup(
    name: String,
    data: DataFrame,
    primaryKeyColumn: String,
    applyMaskOnId: Boolean = true)
    extends PropertyGroup {

  override protected def validate(): this.type = {
    if (!data.columns.contains(primaryKeyColumn)) {
      throw new InvalidPropertyGroupException(
        s"source column $primaryKeyColumn does not exist, existed columns [${data.columns.mkString(", ")}]")
    }
    this
  }

  private[graphframes] def internalIdMapping: DataFrame = data
    .select(col(primaryKeyColumn).alias(PropertyGraphFrame.EXTERNAL_ID))
    .withColumn(
      GraphFrame.ID,
      concat(lit(name), sha2(col(PropertyGraphFrame.EXTERNAL_ID).cast(StringType), 256)))

  override protected[graphframes] def getData(filter: Column): DataFrame = {
    val filteredData = data
      .filter(filter)
    val withId = if (applyMaskOnId) {
      filteredData.select(
        concat(lit(name), sha2(col(primaryKeyColumn).cast(StringType), 256)).alias(GraphFrame.ID))
    } else {
      filteredData.select(col(primaryKeyColumn).cast(StringType).alias(GraphFrame.ID))
    }

    withId.select(col(GraphFrame.ID), lit(name).alias(PropertyGraphFrame.PROPERTY_GROUP_COL_NAME))
  }
}

object VertexPropertyGroup {

  /**
   * Creates a new VertexPropertyGroup with a specified primary key column.
   *
   * @param name
   *   Name of the vertex property group
   * @param data
   *   DataFrame containing vertex data
   * @param primaryKeyColumn
   *   Name of the column to be used as a primary key for vertex identification
   * @return
   *   A validated VertexPropertyGroup instance
   */
  def apply(name: String, data: DataFrame, primaryKeyColumn: String): VertexPropertyGroup =
    new VertexPropertyGroup(name, data, primaryKeyColumn).validate()

  /**
   * Creates a new VertexPropertyGroup using default a primary key column name.
   *
   * @param name
   *   Name of the vertex property group
   * @param data
   *   DataFrame containing vertex data
   * @return
   *   A validated VertexPropertyGroup instance
   */
  def apply(name: String, data: DataFrame): VertexPropertyGroup =
    new VertexPropertyGroup(name, data, GraphFrame.ID)
}
