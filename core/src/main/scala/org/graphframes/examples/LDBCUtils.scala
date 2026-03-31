package org.graphframes.examples

import java.nio.file.*
import scala.sys.process.*

object LDBCUtils {
  private val LDBC_URL_PREFIX = "https://datasets.ldbcouncil.org/graphalytics/"

  val TEST_BFS_DIRECTED = "test-bfs-directed"
  val TEST_BFS_UNDIRECTED = "test-bfs-undirected"
  val TEST_CDLP_DIRECTED = "test-cdlp-directed"
  val TEST_CDLP_UNDIRECTED = "test-cdlp-undirected"
  val TEST_PR_DIRECTED = "test-pr-directed"
  val TEST_PR_UNDIRECTED = "test-pr-undirected"
  val TEST_WCC_DIRECTED = "test-wcc-directed"
  val TEST_WCC_UNDIRECTED = "test-wcc-undirected"
  val KGS = "kgs"
  val GRAPH500_22 = "graph500-22"
  val GRAPH500_23 = "graph500-23"
  val GRAPH500_24 = "graph500-24"
  val GRAPH500_25 = "graph500-25"
  val GRAPH500_26 = "graph500-26"
  val GRAPH500_27 = "graph500-27"
  val GRAPH500_28 = "graph500-28"
  val GRAPH500_29 = "graph500-29"
  val GRAPH500_30 = "graph500-30"
  val CIT_PATENTS = "cit-Patents"
  val WIKI_TALKS = "wiki-Talk"

  private val possibleCaseNames = Set(
    TEST_BFS_DIRECTED,
    TEST_BFS_UNDIRECTED,
    TEST_CDLP_DIRECTED,
    TEST_CDLP_UNDIRECTED,
    TEST_PR_DIRECTED,
    TEST_PR_UNDIRECTED,
    TEST_WCC_DIRECTED,
    TEST_WCC_UNDIRECTED,
    KGS,
    GRAPH500_22,
    GRAPH500_23,
    GRAPH500_24,
    GRAPH500_25,
    GRAPH500_26,
    GRAPH500_27,
    GRAPH500_28,
    GRAPH500_29,
    GRAPH500_30,
    CIT_PATENTS,
    WIKI_TALKS)

  private def ldbcURL(caseName: String): String = s"${LDBC_URL_PREFIX}${caseName}.tar.zst"

  private def checkZSTD(): Unit = {
    try {
      val version = "zstd --version".!
      println(s"found zstd version: $version")
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
        Files.createDirectories(dir)
      }
      val archivePath = path.resolve(s"${name}.tar.zst")
      // Use curl instead of Java's URLConnection because the LDBC CDN (Cloudflare)
      // rejects Java 8's TLS fingerprint with HTTP 403.
      val curlExit = s"curl -fSL -o ${archivePath.toString} ${ldbcURL(name)}".!
      if (curlExit != 0) {
        throw new RuntimeException(s"Failed to download ${ldbcURL(name)} (curl exit code: $curlExit)")
      }
      println(s"Uncompressing ${archivePath.toString} to ${dir.toString}...")
      s"zstd -d ${archivePath.toString} -o ${archivePath.toString.replace(".zst", "")}".!
      s"tar -xf ${archivePath.toString.replace(".zst", "")} -C ${dir.toString}".!

      // Clean up
      Files.delete(archivePath)
      Files.delete(Paths.get(archivePath.toString.replace(".zst", "")))
    }
  }
}
