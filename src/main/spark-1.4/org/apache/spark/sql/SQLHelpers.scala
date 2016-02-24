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

package org.apache.spark.sql

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.SqlParser
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{DataType, LongType}

object SQLHelpers {
  def getExpr(col: Column): Expression = col.expr

  def expr(e: String): Column = new Column(new SqlParser().parseExpression(e))

  def monotonicallyIncreasingId(): Column = Column(PatchedMonotonicallyIncreasingID())

  /**
   * A patched version of [[org.apache.spark.sql.execution.expressions.MonotonicallyIncreasingID()]]
   * that works for local relations in Spark 1.4. (SPARK-9020)
   */
  private case class PatchedMonotonicallyIncreasingID() extends LeafExpression {

    /**
     * Record ID within each partition. By being transient, count's value is reset to 0 every time
     * we serialize and deserialize it.
     */
    @transient private[this] var count: Long = 0L

    override type EvaluatedType = Long

    override def nullable: Boolean = false

    override def dataType: DataType = LongType

    override def eval(input: Row): Long = {
      val currentCount = count
      count += 1
      val taskContext = TaskContext.get()
      if (taskContext == null) {
        // This is a local relation.
        currentCount
      } else {
        (taskContext.partitionId().toLong << 33) + currentCount
      }
    }
  }
}
