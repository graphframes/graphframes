package org.graphframes.propertygraph.property

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame

trait PropertyGroup {
  val name: String
  val data: DataFrame
  protected def validate(): this.type

  /**
   * Maintains a mapping between external IDs and internal hashed IDs used in GraphFrame
   * conversion.
   *
   * When converting multiple edge groups to a GraphFrame, we need to ensure there are no
   * collisions between source/destination vertices from different groups. This is achieved by:
   *   1. Creating a hash of the vertex IDs combined with group name
   *   2. Using these hashed values instead of original edge IDs in the GraphFrame
   *   3. Storing this mapping internally to enable conversion back to original IDs
   */
  protected[graphframes] def internalIdMapping: DataFrame
  protected[graphframes] def getData(): DataFrame = getData(Seq.empty[Column])
  protected[graphframes] def getData(filters: Seq[Column]): DataFrame
}
