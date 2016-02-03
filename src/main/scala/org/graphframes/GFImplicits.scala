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
import org.apache.spark.sql.functions._

object GFImplicits {

  implicit class MotifIterables(cols: Iterable[String]) {
    def field(f: String): Iterable[Column] = cols.map(c => col(c + "." + f))
  }

  implicit class MotifArrays(cols: Array[String]) {
    def field(f: String): Array[Column] = cols.map(c => col(c + "." + f))
  }

}
