package org.graphframes.benchmarks

import java.nio.file.Files
import java.nio.file.Path
import scala.sys.process.*

class ParquetDataLoader(cacheDir: Path) {
  private val LDBC_PARQUET_URL_PREFIX = "https://datasets.ldbcouncil.org/graphalytics-parquet/"

  def downloadParquetIfNotExists(graphName: String): Unit = {
    val graphDir = cacheDir.resolve(graphName)

    if (Files.notExists(graphDir)) {
      Files.createDirectories(graphDir)
    }

    val vertexFile = graphDir.resolve(s"${graphName}-v.parquet")
    val edgeFile = graphDir.resolve(s"${graphName}-e.parquet")

    if (Files.notExists(vertexFile)) {
      println(s"Downloading vertex file for $graphName...")
      downloadFile(s"${LDBC_PARQUET_URL_PREFIX}${graphName}-v.parquet", vertexFile)
    } else {
      println(s"Vertex file for $graphName already exists, skipping download")
    }

    if (Files.notExists(edgeFile)) {
      println(s"Downloading edge file for $graphName...")
      downloadFile(s"${LDBC_PARQUET_URL_PREFIX}${graphName}-e.parquet", edgeFile)
    } else {
      println(s"Edge file for $graphName already exists, skipping download")
    }
  }

  // Use curl instead of Java's URLConnection because the LDBC CDN (Cloudflare)
  // rejects Java 8's TLS fingerprint with HTTP 403.
  private def downloadFile(url: String, dest: Path): Unit = {
    val curlExit = s"curl -fSL -o ${dest.toString} $url".!
    if (curlExit != 0) {
      throw new RuntimeException(s"Failed to download $url (curl exit code: $curlExit)")
    }
    println(s"Downloaded $url to $dest")
  }
}

object ParquetDataLoader {
  val weightedGraphs: Set[String] = Set("kgs")

  val availableGraphs: Set[String] = Set(
    "test-bfs-directed",
    "test-bfs-undirected",
    "test-cdlp-directed",
    "test-cdlp-undirected",
    "test-pr-directed",
    "test-pr-undirected",
    "test-wcc-directed",
    "test-wcc-undirected",
    "kgs",
    "graph500-22",
    "graph500-23",
    "graph500-24",
    "graph500-25",
    "graph500-26",
    "graph500-27",
    "graph500-28",
    "graph500-29",
    "graph500-30",
    "cit-Patents",
    "wiki-Talk")
}
