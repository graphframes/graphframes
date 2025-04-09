package org.graphframes.examples

import java.net.URL
import java.nio.file._
import java.util.Properties

import scala.sys.process._

import org.graphframes.GraphFrame

object LDBCUtils {
  private val LDBC_URL_PREFIX = "https://datasets.ldbcouncil.org/graphalytics/"
  private val bufferSize = 8192 // 8Kb

  val TEST_BFS_DIRECTED = "test-bfs-directed"
  val TEST_BFS_UNDIRECTED = "test-bfs-undirected"
  val TEST_CDLP_DIRECTED = "test-cdlp-directed"
  val TEST_CDLP_UNDIRECTED = "test-cdlp-undirected"
  val TEST_PR_DIRECED = "test-pr-directed"
  val TEST_PR_UNDIRECTED = "test-pr-undirected"
  val TEST_WCC_DIRECTED = "test-wcc-directed"
  val TEST_WCC_UNDIRECTED = "test-wcc-undirected"
  val KGS = "gps"

  private val possibleCaseNames = Set(
    TEST_BFS_DIRECTED,
    TEST_BFS_UNDIRECTED,
    TEST_CDLP_DIRECTED,
    TEST_CDLP_UNDIRECTED,
    TEST_PR_DIRECED,
    TEST_PR_UNDIRECTED,
    TEST_WCC_DIRECTED,
    TEST_WCC_UNDIRECTED,
    KGS)

  def ldbcURL(caseName: String): URL = new URL(s"${LDBC_URL_PREFIX}${caseName}.tar.zst")

  private def checkZSTD(): Unit = {
    try {
      s"zstd --version".!
    } catch {
      case e: Exception =>
        throw new RuntimeException(
          "zstd is not available or not found. Please install zstd and try again.",
          e)
    }
  }

  private def checkName(name: String): Unit = {
    require(
      possibleCaseNames.contains(name),
      s"Wrong ${name}, possible names: ${possibleCaseNames.mkString(", ")}")
  }

  def downloadLDBCIfNotExists(path: Path, name: String): Unit = {
    checkName(name)
    val dir = path.resolve(name)
    if (Files.notExists(dir) || (Files.list(dir).count() == 0L)) {
      println(s"LDBC data for the case ${name} not found. Downloading...")
      checkZSTD()
      if (Files.notExists(dir)) {
        Files.createDirectory(dir)
      }
      val archivePath = path.resolve(s"${name}.tar.zst")
      val connection = ldbcURL(name).openConnection()
      val inputStream = connection.getInputStream
      val outputStream = Files.newOutputStream(archivePath)
      val buffer = new Array[Byte](bufferSize)
      var bytesRead = 0
      while ({ bytesRead = inputStream.read(buffer); bytesRead } != -1) {
        outputStream.write(buffer, 0, bytesRead)
      }
      inputStream.close()
      outputStream.close()
      println(s"Uncompressing ${archivePath.toString} to ${dir.toString}...")
      s"zstd -d ${archivePath.toString} -o ${archivePath.toString.replace(".zst", "")}".!
      s"tar -xf ${archivePath.toString.replace(".zst", "")} -C ${dir.toString}".!

      // Clean up
      Files.delete(archivePath)
      Files.delete(Paths.get(archivePath.toString.replace(".zst", "")))
    }
  }

  private def getProps(name: String, resourcesDir: Path): Properties = {
    checkName(name)
    val propsFile = resourcesDir.resolve(s"${name}.properties")
    val inputStream = Files.newInputStream(propsFile)
    val properties = new Properties()
    properties.load(inputStream)
    inputStream.close()
    properties
  }
}
