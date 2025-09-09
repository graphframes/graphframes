package org.graphframes.propertygraph.property

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

trait PropertyGroup {
  val name: String
  val data: DataFrame
  protected def validate(): this.type

  /**
   * Returns a view of the data for the property group without applying any filter.
   *
   * @return
   *   A DataFrame containing the raw data.
   */
  protected[graphframes] def getData(): DataFrame = getData(lit(true))

  /**
   * Returns a filtered view of the data for the property group, with an optional mask applied to
   * IDs.
   *
   * @param filter
   *   A condition (Column) used to filter the data.
   * @return
   *   A DataFrame containing the filtered and optionally transformed data.
   */
  protected[graphframes] def getData(filter: Column): DataFrame
}
