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

import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.sql.functions.*
import org.graphframes.*

import scala.collection.mutable

class JobDescriptionSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  import sqlImplicits.*

  private val descriptionKey = "spark.job.description"

  private def chainGraph(n: Int = 5): GraphFrame = {
    val vertices = (1 to n).toDF("id")
    val edges = (1 until n).map(x => (x, x + 1)).toDF("src", "dst")
    GraphFrame(vertices, edges)
  }

  private def propagationPregel(graph: GraphFrame, maxIter: Int): Pregel = graph.pregel
    .setMaxIter(maxIter)
    .withVertexColumn(
      "value",
      when(col("id") === lit(1), lit(1)).otherwise(lit(0)),
      greatest(col("value"), coalesce(Pregel.msg, lit(0))))
    .sendMsgToDst(Pregel.src("value"))
    .aggMsgs(max(Pregel.msg))

  /**
   * Collects the job descriptions of all jobs started while running `body`. Listener events are
   * delivered asynchronously, so after `body` completes this polls until `await` is satisfied by
   * the captured descriptions (or a timeout is reached).
   */
  private def capturedDescriptionsDuring(body: => Unit)(
      await: Seq[String] => Boolean): Seq[String] = {
    val captured = mutable.ArrayBuffer.empty[String]
    val listener = new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        val description = jobStart.properties.getProperty(descriptionKey)
        if (description != null) {
          captured.synchronized {
            val _ = captured += description
          }
        }
      }
    }
    sc.addSparkListener(listener)
    try {
      body
      val deadline = System.currentTimeMillis() + 30000L
      while (System.currentTimeMillis() < deadline
        && !captured.synchronized(await(captured.toSeq))) {
        Thread.sleep(50)
      }
      captured.synchronized(captured.toSeq)
    } finally {
      sc.removeSparkListener(listener)
    }
  }

  test("Pregel sets a job description for each iteration") {
    val descriptions = capturedDescriptionsDuring {
      val _ = propagationPregel(chainGraph(), maxIter = 2).run()
    }(_.contains("GraphFrames Pregel: materializing final result"))

    assert(descriptions.contains("GraphFrames Pregel: iteration 1 / 2"))
    assert(descriptions.contains("GraphFrames Pregel: iteration 2 / 2"))
    assert(descriptions.contains("GraphFrames Pregel: materializing final result"))
  }

  test("Pregel job description prefix is configurable") {
    val descriptions = capturedDescriptionsDuring {
      val _ = propagationPregel(chainGraph(), maxIter = 1)
        .setJobDescriptionPrefix("my Pregel run")
        .run()
    }(_.contains("my Pregel run: materializing final result"))

    assert(descriptions.contains("my Pregel run: iteration 1 / 1"))
    assert(descriptions.forall(!_.startsWith("GraphFrames Pregel")))
  }

  test("Pregel restores the caller's job description") {
    sc.setJobDescription("caller description")
    try {
      val vertices = propagationPregel(chainGraph(), maxIter = 1).run()
      assert(vertices.count() === 5)
      assert(sc.getLocalProperty(descriptionKey) === "caller description")

      // The description must be restored on failures as well.
      val _ = intercept[Exception] {
        propagationPregel(chainGraph(), maxIter = 1)
          .aggMsgs(max(col("nonexistent")))
          .run()
      }
      assert(sc.getLocalProperty(descriptionKey) === "caller description")
    } finally {
      sc.setLocalProperty(descriptionKey, null)
    }
  }

  test("Pregel-based algorithms set their own job description prefix") {
    val descriptions = capturedDescriptionsDuring {
      val _ = chainGraph().shortestPaths
        .landmarks(Seq(5))
        .setAlgorithm("graphframes")
        .run()
    }(_.exists(_.startsWith("GraphFrames ShortestPaths: iteration")))

    assert(descriptions.exists(_.startsWith("GraphFrames ShortestPaths: iteration 1")))
  }

  test("maximal independent set sets job descriptions and restores the caller's") {
    sc.setJobDescription("caller description")
    try {
      val descriptions = capturedDescriptionsDuring {
        val _ = chainGraph().maximalIndependentSet.run(seed = 12345L)
      }(_.contains("GraphFrames MaximalIndependentSet: materializing final result"))

      assert(descriptions.exists(_.startsWith("GraphFrames MaximalIndependentSet: iteration ")))
      assert(sc.getLocalProperty(descriptionKey) === "caller description")
    } finally {
      sc.setLocalProperty(descriptionKey, null)
    }
  }

  test("random walks set per-batch job descriptions and restore the caller's") {
    val temporaryPrefix = java.nio.file.Files.createTempDirectory("rw-job-descriptions").toString
    val rwRunner = new org.graphframes.rw.RandomWalkWithRestart()
      .onGraph(chainGraph())
      .setNumBatches(2)
      .setBatchSize(2)
      .setNumWalksPerNode(1)
      .setTemporaryPrefix(temporaryPrefix)

    sc.setJobDescription("caller description")
    try {
      val descriptions = capturedDescriptionsDuring {
        val _ = rwRunner.run()
      }(_.exists(_.endsWith(": materializing final result")))

      assert(descriptions.exists(d =>
        d.startsWith("GraphFrames RandomWalkWithRestart [") && d.endsWith(": batch 1 of 2")))
      assert(
        descriptions.exists(d =>
          d.startsWith("GraphFrames RandomWalkWithRestart [")
            && d.endsWith(": materializing final result")))
      assert(sc.getLocalProperty(descriptionKey) === "caller description")
    } finally {
      sc.setLocalProperty(descriptionKey, null)
      rwRunner.cleanUp()
    }
  }

  // broadcastThreshold -1 selects the AQE-based two-phase implementation
  Seq(("two_phase", 1000000), ("two_phase", -1), ("randomized_contraction", 1000000)).foreach {
    case (algorithm, broadcastThreshold) =>
      test(
        s"connected components ($algorithm, broadcastThreshold=$broadcastThreshold) " +
          "sets job descriptions and restores the caller's") {
        sc.setJobDescription("caller description")
        try {
          val descriptions = capturedDescriptionsDuring {
            val _ = chainGraph().connectedComponents
              .setAlgorithm(algorithm)
              .setBroadcastThreshold(broadcastThreshold)
              .run()
          }(_.exists(_.contains(": materializing final result")))

          assert(descriptions.exists(d =>
            d.startsWith("GraphFrames ConnectedComponents [") && d.contains(": iteration ")))
          assert(
            descriptions.exists(d =>
              d.startsWith("GraphFrames ConnectedComponents [")
                && d.endsWith(": materializing final result")))
          assert(sc.getLocalProperty(descriptionKey) === "caller description")
        } finally {
          sc.setLocalProperty(descriptionKey, null)
        }
      }
  }
}
