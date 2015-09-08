package com.databricks.dfgraph

import java.io.{IOException, File}
import java.util.UUID

import org.apache.hadoop.util.ShutdownHookManager
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SQLContext
import org.scalatest.FunSuite

class DFGraphSuite extends FunSuite with LocalSparkContext {
  test("instantiate DFGraph from VertexRDD and EdgeRDD") {
    withSpark { sc =>
      val ring = (0L to 100L).zip((1L to 99L) :+ 0L)
      val doubleRing = ring ++ ring
      val graph = Graph.fromEdgeTuples(sc.parallelize(doubleRing), 1)

      val vtxRdd = graph.vertices
      val edgeRdd = graph.edges
      val graphBuiltFromDFGraph = DFGraph(vtxRdd, edgeRdd).toGraph()

      assert(graphBuiltFromDFGraph.vertices.count() === graph.vertices.count())
      assert(graphBuiltFromDFGraph.edges.count() === graph.edges.count())
    }
  }

  test("import/export") {
    withSpark { sc =>
      val ring = (0L to 100L).zip((1L to 99L) :+ 0L)
      val doubleRing = ring ++ ring
      val graph = Graph.fromEdgeTuples(sc.parallelize(doubleRing), 1)
      val dfGraph = DFGraph(graph)

      val tempDir1 = createTempDir()
      val path1 = tempDir1.toURI.toString
      dfGraph.save(path1)

      val sqlContext = SQLContext.getOrCreate(sc)
      val loadedDfGraph = DFGraph.load[Int, Int](sqlContext, path1)
      assert(loadedDfGraph.vertexDF.collect() === dfGraph.vertexDF.collect())
      assert(loadedDfGraph.edgeDF.collect() === dfGraph.edgeDF.collect())
    }
  }


  // TODO: reuse spark.util.Utils
  // TODO: clean up directory after tests finish
  /**
   * Create a temporary directory inside the given parent directory. The directory will be
   * automatically deleted when the VM shuts down.
   */
  private def createTempDir(
      root: String = System.getProperty("java.io.tmpdir"),
      namePrefix: String = "spark"): File = {
    val dir = createDirectory(root, namePrefix)
//    ShutdownHookManager.registerShutdownDeleteDir(dir)
    dir
  }

  /**
   * Create a directory inside the given parent directory. The directory is guaranteed to be
   * newly created, and is not marked for automatic deletion.
   */
  def createDirectory(root: String, namePrefix: String = "spark"): File = {
    var attempts = 0
    val maxAttempts = 10
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case e: SecurityException => dir = null; }
    }
    dir.getCanonicalFile
  }
}

