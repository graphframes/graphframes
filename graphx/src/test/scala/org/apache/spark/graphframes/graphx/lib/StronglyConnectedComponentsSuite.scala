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

package org.apache.spark.graphframes.graphx.lib

import org.apache.spark.graphframes.graphx.*
import org.apache.spark.graphframes.graphx.LocalSparkContext
import org.scalatest.funsuite.AnyFunSuite

class StronglyConnectedComponentsSuite extends AnyFunSuite with LocalSparkContext {

  test("Island Strongly Connected Components") {
    withSpark { sc =>
      val vertices = sc.parallelize((1L to 5L).map(x => (x, -1)))
      val edges = sc.parallelize(Seq.empty[Edge[Int]])
      val graph = Graph[Int, Int](vertices, edges)
      val sccGraph = graph.stronglyConnectedComponents(5)
      for ((id, scc) <- sccGraph.vertices.collect()) {
        assert(id === scc)
      }
    }
  }

  test("Cycle Strongly Connected Components") {
    withSpark { sc =>
      val rawEdges = sc.parallelize((0L to 6L).map(x => (x, (x + 1) % 7)))
      val graph = Graph.fromEdgeTuples(rawEdges, -1)
      val sccGraph = graph.stronglyConnectedComponents(20)
      for ((_, scc) <- sccGraph.vertices.collect()) {
        assert(0L === scc)
      }
    }
  }

  test("2 Cycle Strongly Connected Components") {
    withSpark { sc =>
      val edges =
        Array(0L -> 1L, 1L -> 2L, 2L -> 0L) ++
          Array(3L -> 4L, 4L -> 5L, 5L -> 3L) ++
          Array(6L -> 0L, 5L -> 7L)
      val rawEdges = sc.parallelize(edges.toIndexedSeq)
      val graph = Graph.fromEdgeTuples(rawEdges, -1)
      val sccGraph = graph.stronglyConnectedComponents(20)
      for ((id, scc) <- sccGraph.vertices.collect()) {
        if (id < 3) {
          assert(0L === scc)
        } else if (id < 6) {
          assert(3L === scc)
        } else {
          assert(id === scc)
        }
      }
    }
  }

}
