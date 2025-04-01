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

package org.apache.spark.sql.graphframes

import org.scalatest.Suite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.classic.{ClassicConversions, ColumnConversions, ColumnNodeToExpressionConverter, SparkSession => ClassicSparkSession, SQLImplicits}

trait SparkTestShims { self: Suite =>
  var spark: SparkSession

  /**
   * A helper object for importing SQL implicits.
   *
   * Note that the alternative of importing `spark.implicits._` is not possible here. This is
   * because we create the `SQLContext` immediately before the first test is run, but the
   * implicits import is needed in the constructor.
   */
  protected object testImplicits
      extends SQLImplicits
      with ClassicConversions
      with ColumnConversions {
    override protected def session: ClassicSparkSession =
      self.spark.asInstanceOf[ClassicSparkSession]
    override protected def converter: ColumnNodeToExpressionConverter = self.spark.converter
  }
}
