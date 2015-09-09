package com.databricks.dfgraph

import java.io.{File, IOException}
import java.util.UUID

import org.apache.commons.lang3.SystemUtils
import org.scalatest.FunSuite

import org.apache.spark.graphx.{EdgeRDD, VertexRDD, Edge, Graph}
import org.apache.spark.sql.SQLContext

class DFGraphSuite extends FunSuite with LocalSparkContext {
  test("instantiate DFGraph from VertexRDD and EdgeRDD with VD=int, ED=int") {
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

  test("instantiate DFGraph from Graph with VD=(Double, Boolean), ED=(Long, String)") {
    withSpark { sc =>
      val ring = (0L to 100L).zip((1L to 99L) :+ 0L)
      val doubleRing = ring ++ ring
      val vertices = VertexRDD(sc.parallelize((0L to 100L).map { case (vtxId) =>
        (vtxId, (vtxId.toDouble, vtxId % 2 == 0))
      }))
       val edges = EdgeRDD.fromEdges[(Long, String), (Double, Boolean)](
        sc.parallelize(doubleRing.map { case (src, dst) =>
          Edge(src, dst, (src, src.toString + dst.toString))
        }))
      val graph = Graph(vertices, edges)

      val graphBuiltFromDFGraph = DFGraph(graph).toGraph()

      assert(graphBuiltFromDFGraph.vertices.count() === graph.vertices.count())
      assert(graphBuiltFromDFGraph.edges.count() === graph.edges.count())
    }
  }

  test("import/export") {
    withSpark { sc =>
      val tempDir = createDirectory(System.getProperty("java.io.tmpdir"), "spark")
      try {
        val ring = (0L to 100L).zip((1L to 99L) :+ 0L)
        val doubleRing = ring ++ ring
        val vertices = VertexRDD(sc.parallelize((0L to 100L).map { case (vtxId) =>
          (vtxId, (vtxId.toDouble, vtxId % 2 == 0))
        }))
       val edges = EdgeRDD.fromEdges[(Long, String), (Double, Boolean)](
        sc.parallelize(doubleRing.map { case (src, dst) =>
          Edge(src, dst, (src, src.toString + dst.toString))
        }))
        val dfGraph = DFGraph(vertices, edges)

        val path = tempDir.toURI.toString
        dfGraph.save(path)

        val sqlContext = SQLContext.getOrCreate(sc)
        val loadedDfGraph = DFGraph.load[Int, Int](sqlContext, path)
        assert(loadedDfGraph.vertexDF.collect() === dfGraph.vertexDF.collect())
        assert(loadedDfGraph.edgeDF.collect() === dfGraph.edgeDF.collect())
      } finally {
        deleteRecursively(tempDir)
      }
    }
  }

  // TODO: reuse code from spark.util.Utils
  /**
   * Create a directory inside the given parent directory. The directory is guaranteed to be
   * newly created, and is not marked for automatic deletion.
   */
  private def createDirectory(root: String, namePrefix: String = "spark"): File = {
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

 /**
   * Delete a file or directory and its contents recursively.
   * Don't follow directories if they are symlinks.
   * Throws an exception if deletion is unsuccessful.
   */
  private def deleteRecursively(file: File) {
    if (file != null) {
      try {
        if (file.isDirectory && !isSymlink(file)) {
          var savedIOException: IOException = null
          for (child <- listFilesSafely(file)) {
            try {
              deleteRecursively(child)
            } catch {
              // In case of multiple exceptions, only last one will be thrown
              case ioe: IOException => savedIOException = ioe
            }
          }
          if (savedIOException != null) {
            throw savedIOException
          }
        }
      } finally {
        if (!file.delete()) {
          // Delete can also fail if the file simply did not exist
          if (file.exists()) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath)
          }
        }
      }
    }
  }

  /**
   * Check to see if file is a symbolic link.
   */
  private def isSymlink(file: File): Boolean = {
    if (file == null) throw new NullPointerException("File must not be null")
    if (SystemUtils.IS_OS_WINDOWS) return false
    val fileInCanonicalDir = if (file.getParent() == null) {
      file
    } else {
      new File(file.getParentFile().getCanonicalFile(), file.getName())
    }

    !fileInCanonicalDir.getCanonicalFile().equals(fileInCanonicalDir.getAbsoluteFile())
  }

  private def listFilesSafely(file: File): Seq[File] = {
    if (file.exists()) {
      val files = file.listFiles()
      if (files == null) {
        throw new IOException("Failed to list files for dir: " + file)
      }
      files
    } else {
      List()
    }
  }
}

