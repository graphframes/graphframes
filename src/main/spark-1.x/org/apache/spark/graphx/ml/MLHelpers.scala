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

package org.apache.spark.ml.linalg

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types._

// private[spark] class MatrixUDT extends UserDefinedType[Double]
// private[spark] class VectorUDT extends UserDefinedType[Double]

/**
 * :: DeveloperApi ::
 * SQL data types for vectors and matrices.
 */
@Since("2.0.0")
@DeveloperApi
object SQLDataTypes {

  /** Data type for [[Vector]]. */
  val VectorType: DataType = NullType

  /** Data type for [[Matrix]]. */
  val MatrixType: DataType = NullType
}

sealed trait Vector
sealed trait Matrix
/**
 * A sparse vector represented by an index array and a value array.
 *
 * @param size size of the vector.
 * @param indices index array, assume to be strictly increasing.
 * @param values value array, must have the same length as the index array.
 */
case class SparseVector(
    size: Int,
    indices: Array[Int],
    values: Array[Double]) extends Vector {
  def numNonzeros: Int = {
    throw new NotImplementedError("SparseVector type is only supported in Spark 2.0+")
  }
}

