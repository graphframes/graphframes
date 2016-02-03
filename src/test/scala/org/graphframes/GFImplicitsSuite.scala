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

import org.apache.spark.sql.Column

import org.graphframes.GFImplicits._

class GFImplicitsSuite extends SparkFunSuite {

  test("MotifIterables: Iterable[String]") {
    val colNames = Seq("a", "b", "c")
    val colFields: Iterable[Column] = colNames.field("age")
    colNames.zip(colFields).foreach { case (c, field) =>
      assert(c + ".age" === field.toString)
    }
  }

  test("MotifIterables: Array[String]") {
    val colNames = Array("a", "b", "c")
    val colFields: Array[Column] = colNames.field("age")
    colNames.zip(colFields).foreach { case (c, field) =>
      assert(c + ".age" === field.toString)
    }
  }
}
