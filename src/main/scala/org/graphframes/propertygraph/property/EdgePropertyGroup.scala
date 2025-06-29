package org.graphframes.propertygraph.property

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.xxhash64
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
 *   - Direction flag indicating if edges are directed or undirected
 *   - Column names specifying source vertex, destination vertex and edge weight columns
 *
 * The class provides multiple factory methods through its companion object to create edge groups
 * with:
 *   - Custom column names for a source, destination and weight
 *   - Default column names ("src", "dst", "weight")
 *   - Automatic weight column generation (default value 1.0)
 *   - Specified directionality
 *
 * The class validates that required columns exist in the provided DataFrame on creation. Required
 * columns are:
 *   - Source vertex column (default: "src")
 *   - Destination vertex column (default: "dst")
 *   - Weight column (default: "weight" with value 1.0)
 *
 * @param name
 *   Unique identifier for this edge property group
 * @param data
 *   DataFrame containing the edge data with required columns
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

  override protected[graphframes] def internalIdMapping: DataFrame = {
    data
      .select(col(srcColumnName).alias(EXTERNAL_ID))
      .union(data.select(col(dstColumnName).alias(EXTERNAL_ID)))
      .distinct()
      .withColumn(GraphFrame.ID, xxhash64(lit(name), col(EXTERNAL_ID)))
  }

  override protected[graphframes] def getData(filters: Seq[Column]): DataFrame = {
    val filteredData = filters.foldLeft(data)((data, filter) => data.filter(filter))

    val baseEdges = filteredData.select(
      xxhash64(lit(name), col(srcColumnName)).alias(GraphFrame.SRC),
      xxhash64(lit(name), col(dstColumnName)).alias(GraphFrame.DST),
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

  /**
   * Creates an EdgePropertyGroup with fully specified parameters
   *
   * @param name
   *   Unique identifier for this property group
   * @param data
   *   Underlying DataFrame containing edge data
   * @param isDirected
   *   Whether edges are directed (true) or undirected (false)
   * @param srcColumnName
   *   Name of source vertex column in data
   * @param dstColumnName
   *   Name of destination vertex column in data
   * @param weightColumnName
   *   Name of edge weight column in data
   * @return
   *   A validated EdgePropertyGroup instance
   */
  def apply(
      name: String,
      data: DataFrame,
      isDirected: Boolean,
      srcColumnName: String,
      dstColumnName: String,
      weightColumnName: String): EdgePropertyGroup = {
    new EdgePropertyGroup(name, data, isDirected, srcColumnName, dstColumnName, weightColumnName)
      .validate()
  }

  /**
   * Creates an EdgePropertyGroup with undirected edges
   *
   * @param name
   *   Unique identifier for this property group
   * @param data
   *   Underlying DataFrame containing edge data
   * @param srcColumnName
   *   Name of source vertex column in data
   * @param dstColumnName
   *   Name of destination vertex column in data
   * @param weightColumnName
   *   Name of edge weight column in data
   * @return
   *   A validated EdgePropertyGroup instance with isDirected=false
   */
  def apply(
      name: String,
      data: DataFrame,
      srcColumnName: String,
      dstColumnName: String,
      weightColumnName: String): EdgePropertyGroup = {
    EdgePropertyGroup(
      name,
      data,
      isDirected = false,
      srcColumnName,
      dstColumnName,
      weightColumnName)
  }

  /**
   * Creates an EdgePropertyGroup with default column names and weights
   *
   * @param name
   *   Unique identifier for this property group
   * @param data
   *   Underlying DataFrame containing edge data
   * @return
   *   A validated EdgePropertyGroup instance with src/dst columns, weight=1.0, isDirected=false
   */
  def apply(name: String, data: DataFrame): EdgePropertyGroup = {
    val dataWithWeight = data.withColumn(GraphFrame.WEIGHT, lit(1.0))
    EdgePropertyGroup(
      name,
      dataWithWeight,
      isDirected = false,
      GraphFrame.SRC,
      GraphFrame.DST,
      GraphFrame.WEIGHT)
  }

  /**
   * Creates an EdgePropertyGroup with default column names and specified direction
   *
   * @param name
   *   Unique identifier for this property group
   * @param data
   *   Underlying DataFrame containing edge data
   * @param isDirected
   *   Whether edges are directed (true) or undirected (false)
   * @return
   *   A validated EdgePropertyGroup instance with src/dst columns and weight=1.0
   */
  def apply(name: String, data: DataFrame, isDirected: Boolean): EdgePropertyGroup = {
    val dataWithWeight = data.withColumn(GraphFrame.WEIGHT, lit(1.0))
    EdgePropertyGroup(
      name,
      dataWithWeight,
      isDirected,
      GraphFrame.SRC,
      GraphFrame.DST,
      GraphFrame.WEIGHT)
  }

  /**
   * Creates an EdgePropertyGroup with specified vertex columns
   *
   * @param name
   *   Unique identifier for this property group
   * @param data
   *   Underlying DataFrame containing edge data
   * @param srcColumnName
   *   Name of source vertex column in data
   * @param dstColumnName
   *   Name of destination vertex column in data
   * @return
   *   A validated EdgePropertyGroup instance with weight=1.0, isDirected=false
   */
  def apply(
      name: String,
      data: DataFrame,
      srcColumnName: String,
      dstColumnName: String): EdgePropertyGroup = {
    val dataWithWeight = data.withColumn(GraphFrame.WEIGHT, lit(1.0))
    EdgePropertyGroup(
      name,
      dataWithWeight,
      isDirected = false,
      srcColumnName,
      dstColumnName,
      GraphFrame.WEIGHT)
  }

  /**
   * Creates an EdgePropertyGroup with specified vertex columns and direction
   *
   * @param name
   *   Unique identifier for this property group
   * @param data
   *   Underlying DataFrame containing edge data
   * @param isDirected
   *   Whether edges are directed (true) or undirected (false)
   * @param srcColumnName
   *   Name of source vertex column in data
   * @param dstColumnName
   *   Name of destination vertex column in data
   * @return
   *   A validated EdgePropertyGroup instance with weight=1.0
   */
  def apply(
      name: String,
      data: DataFrame,
      isDirected: Boolean,
      srcColumnName: String,
      dstColumnName: String): EdgePropertyGroup = {
    val dataWithWeight = data.withColumn(GraphFrame.WEIGHT, lit(1.0))
    EdgePropertyGroup(
      name,
      dataWithWeight,
      isDirected,
      srcColumnName,
      dstColumnName,
      GraphFrame.WEIGHT)
  }

  /**
   * Creates an EdgePropertyGroup with a specified weight column
   *
   * @param name
   *   Unique identifier for this property group
   * @param data
   *   Underlying DataFrame containing edge data
   * @param weightColumnName
   *   Name of edge weight column in data
   * @return
   *   A validated EdgePropertyGroup instance with default src/dst columns, isDirected=false
   */
  def apply(name: String, data: DataFrame, weightColumnName: String): EdgePropertyGroup = {
    EdgePropertyGroup(
      name,
      data,
      isDirected = false,
      GraphFrame.SRC,
      GraphFrame.DST,
      weightColumnName)
  }

  /**
   * Creates an EdgePropertyGroup with specified weight column and direction
   *
   * @param name
   *   Unique identifier for this property group
   * @param data
   *   Underlying DataFrame containing edge data
   * @param isDirected
   *   Whether edges are directed (true) or undirected (false)
   * @param weightColumnName
   *   Name of edge weight column in data
   * @return
   *   A validated EdgePropertyGroup instance with default src/dst columns
   */
  def apply(
      name: String,
      data: DataFrame,
      isDirected: Boolean,
      weightColumnName: String): EdgePropertyGroup = {
    EdgePropertyGroup(name, data, isDirected, GraphFrame.SRC, GraphFrame.DST, weightColumnName)
  }
}
