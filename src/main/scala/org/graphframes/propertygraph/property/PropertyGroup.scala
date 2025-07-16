package org.graphframes.propertygraph.property

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

trait PropertyGroup {
  val name: String
  val data: DataFrame
  protected def validate(): this.type

  protected[graphframes] def getData: DataFrame = getData(lit(true))
  protected[graphframes] def getData(filter: Column): DataFrame
}
