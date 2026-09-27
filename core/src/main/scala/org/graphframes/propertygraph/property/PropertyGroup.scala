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
  private[propertygraph] def getData(): DataFrame = getData(lit(true))

  /**
   * Returns a filtered view of the data for the property group without requesting any extra
   * property columns (only id/standardized columns are projected). Equivalent to
   * `getData(filter, Seq.empty)`.
   *
   * @param filter
   *   A condition (Column) used to filter the data.
   * @return
   *   A DataFrame containing the filtered and optionally transformed data.
   */
  private[propertygraph] def getData(filter: Column): DataFrame = getData(filter, Seq.empty)

  /**
   * Returns a filtered view of the data for the property group, with an optional mask applied to
   * IDs, and additionally carrying the named property columns through to the output.
   *
   * The extra `requestedProperties` are useful for query engines that need to surface specific
   * properties (e.g. a `RETURN a.age` projection) without re-reading the raw `data`. They are
   * passed through unmodified — they are not join keys and are never masked. When
   * `requestedProperties` is empty, the output is identical to `getData(filter)`.
   *
   * @param filter
   *   A condition (Column) used to filter the data.
   * @param requestedProperties
   *   Names of additional property columns to carry through to the output.
   * @return
   *   A DataFrame containing the filtered, optionally transformed data plus requested properties.
   */
  private[propertygraph] def getData(filter: Column, requestedProperties: Seq[String]): DataFrame

  /** Convenience overload of [[getData(filter, requestedProperties)* ]] with no filter. */
  private[propertygraph] def getData(requestedProperties: Seq[String]): DataFrame =
    getData(lit(true), requestedProperties)
}
