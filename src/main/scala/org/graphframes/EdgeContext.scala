/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.graphframes

import org.apache.spark.sql.Row

class DataNotAvailableException
  extends RuntimeException("The data is not available. Change the edge selection")

trait EdgeContext {
  // Because GraphFrame is untyped, there is no easy way to express typed operations like this.
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


private[graphframes] class EdgeContextImpl(
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