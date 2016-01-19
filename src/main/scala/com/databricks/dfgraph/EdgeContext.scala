package com.databricks.dfgraph

import org.apache.spark.sql.Row

class DataNotAvailableException extends RuntimeException("The data is not available. Change the EdgePartition selection")

trait EdgeContext {
  // Because DFGraph is untyped, there is no easy way to express typed operations such as this one.
  // We coerce the type to the underlying GraphX representation (Long)
  def sourceVertexId: Long

  def destinationVertexId: Long

  @throws[DataNotAvailableException]("when missing data")
  def sourceVertex: Row

  @throws[DataNotAvailableException]("when missing data")
  def destinationVertex: Row

  @throws[DataNotAvailableException]("when missing data")
  def edge: Row
}


private[dfgraph] class EdgeContextImpl(
    override val sourceVertexId: Long,
    override val destinationVertexId: Long,
    private val src: Row,
    private val dest: Row,
    private val e: Row) extends EdgeContext {

  private def check(r: Row): Row = Option(r).getOrElse( throw new DataNotAvailableException)

  override def sourceVertex = check(src)
  override def destinationVertex = check(dest)
  override def edge = check(e)
}