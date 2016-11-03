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

import java.io.File
import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, Suite}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

trait GraphFrameTestSparkContext extends BeforeAndAfterAll { self: Suite =>
  @transient var sc: SparkContext = _
  @transient var sqlContext: SQLContext = _

  override def beforeAll() {
    super.beforeAll()
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("GraphFramesUnitTest")
      .set("spark.sql.shuffle.partitions", "4")  // makes small tests much faster
    sc = new SparkContext(conf)
    val checkpointDir = Files.createTempDirectory(this.getClass.getName).toString
    sc.setCheckpointDir(checkpointDir)
    sqlContext = new SQLContext(sc)
  }

  override def afterAll() {
    val checkpointDir = sc.getCheckpointDir
    sqlContext = null
    if (sc != null) {
      sc.stop()
    }
    sc = null
    checkpointDir.foreach { dir =>
      FileUtils.deleteQuietly(new File(dir))
    }
    super.afterAll()
  }
}
