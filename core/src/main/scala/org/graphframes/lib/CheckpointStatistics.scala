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

package org.graphframes.lib

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.graphframes.SparkShims
import org.apache.spark.sql.internal.SQLConf
import org.graphframes.Logging

private[graphframes] object CheckpointStatistics extends Logging {

  /**
   * Spark copies the size estimate of the checkpointed plan onto the checkpoint relation. When an
   * iterative algorithm joins that relation again and checkpoints the result, the estimate is
   * multiplied on every checkpoint. Once it exceeds the default size of an unknown relation, it
   * carries no information but keeps growing, and planners doing arithmetic on it can spend
   * minutes per query. In that case the checkpointed data is re-exposed through the public API,
   * which drops the estimate (and the output partitioning metadata) while keeping the
   * checkpointed RDD lineage.
   */
  def dropOversized(checkpointed: DataFrame, context: => String): DataFrame = {
    val stats = SparkShims.planFromDataFrame(checkpointed).stats
    val limit = BigInt(SQLConf.get.defaultSizeInBytes)
    if (stats.sizeInBytes > limit || stats.rowCount.exists(_ > limit)) {
      logInfo(
        s"dropping oversized checkpoint statistics at $context " +
          s"(sizeInBytes has ${stats.sizeInBytes.bitLength} bits)")
      checkpointed.sparkSession.createDataFrame(checkpointed.rdd, checkpointed.schema)
    } else {
      checkpointed
    }
  }
}
