import io.circe.Json
import io.circe.parser.parse
import laika.ast.*
import laika.ast.Path.Root
import laika.config.LaikaKeys
import laika.config.LinkConfig
import laika.config.SourceLinks
import laika.helium.Helium
import laika.helium.config.*
import laika.sbt.LaikaConfig
import laika.theme.ThemeProvider

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import scala.util.Try
import scala.util.Using
import scala.util.matching.Regex

import scala.collection.JavaConverters.*

object LaikaCustoms {
  private val gfDescription: String = "Scalable Graph Processing on top of Apache Spark"
  val thisVersionShortRegex: Regex = """^([0-9]+\.[0-9]+\.[0-9]+)(.*)$""".r

  def generateAtomFeed(blogDir: Path, baseUrl: String): Unit = {
    val posts = Files
      .walk(blogDir)
      .iterator()
      .asScala
      .filter(_.getFileName.toString.endsWith(".md"))
      .filter(_.getFileName.toString != "01-index.md")
      .toSeq

    val titleRegex = """(?m)^-\s\*\*Title:\*\*\s(.+)$""".r
    val publishedRegex = """(?m)^-\s\*\*Published:\*\*\s(.+)$""".r
    val summaryRegex = """(?m)^-\s\*\*Summary:\*\*\s(.+)$""".r

    def genEntry(
        title: String,
        link: String,
        updated: String,
        id: String,
        summary: String): String = {
      s"""
         |\t<entry>
         |\t\t<title>$title</title>
         |\t\t<link href="$link"/>
         |\t\t<id>$id</id>
         |\t\t<updated>$updated</updated>
         |\t\t<summary>$summary</summary>
         |\t</entry>
         |
         |""".stripMargin
    }

    val collected =
      posts
        .sortBy(_.getFileName.toString)
        .foldLeft((new StringBuilder(), "1970-01-01:T00:00:00Z")) { (acc, post) =>
          {
            println(s"Generating feed entry for ${post.getFileName}")
            val content = Using(scala.io.Source.fromFile(post.toFile)) { source =>
              source.mkString
            }
              .getOrElse("")

            val id =
              java.util.UUID
                .nameUUIDFromBytes(post.getFileName.toString.getBytes("UTF-8"))
                .toString
            val title = titleRegex.findAllMatchIn(content) match {
              case titleMatch if titleMatch.hasNext => titleMatch.next().group(1)
              case _ => "No title"
            }
            val link =
              s"$baseUrl/05-blog/${post.getFileName.toString.replaceAll("\\.md", "\\.html")}"
            val updated = publishedRegex.findAllMatchIn(content) match {
              case publishedMatch if publishedMatch.hasNext => publishedMatch.next().group(1)
              case _ => "1970-01-01:T00:00:00Z"
            }
            val summary = summaryRegex.findAllMatchIn(content) match {
              case summaryMatch if summaryMatch.hasNext => summaryMatch.next().group(1)
              case _ => "No summary"
            }
            val builderWithPost = acc._1.append(genEntry(title, link, updated, id, summary))
            if (updated > acc._2) {
              (builderWithPost, updated)
            } else {
              (builderWithPost, acc._2)
            }
          }
        }

    val header =
      s"""<?xml version="1.0" encoding="utf-8"?>
        |<feed xmlns="http://www.w3.org/2005/Atom">
        |
        |\t<title>GraphFrames Blog</title>
        |\t<id>${baseUrl}</id>
        |\t<link href="${baseUrl}/05-blog/}/"/>
        |\t<updated>${collected._2}</updated>
        |
        |""".stripMargin

    val feedContent = header + collected._1.append("</feed>").mkString
    val feedFile = blogDir.resolve("feed.xml")
    Files.write(feedFile, feedContent.getBytes)
  }

  def laikaConfig(benchmarksFile: Path): LaikaConfig = {
    val baseConfig = LaikaConfig.defaults.withRawContent
      .withConfigValue(LaikaKeys.site.apiPath, "api/scaladoc")
      .withConfigValue("github.baseUri", "https://github.com/graphframes/graphframes")
      .withConfigValue(
        LinkConfig.empty.addSourceLinks(SourceLinks(
          baseUri = "https://github.com/graphframes/graphframes/tree/master/core/src/main/scala/",
          suffix = "scala").withPackagePrefix("org.graphframes")))

    if (!Files.exists(benchmarksFile)) {
      println(s"File $benchmarksFile does not exist. Skipping.")
      return baseConfig
    }
    Using(scala.io.Source.fromFile(benchmarksFile.toFile)) { source =>
      {
        parse(source.mkString)
          .getOrElse(Json.Null)
          .asArray
          .map(array =>
            array.foldLeft(baseConfig) { (config, bench) =>
              {
                val name =
                  bench.hcursor.downField("benchmark").as[String].getOrElse("").split("\\.").last
                val measurements =
                  bench.hcursor.downField("measurementIterations").as[Int].getOrElse(-1)
                val metric = bench.hcursor
                  .downField("primaryMetric")
                  .downField("score")
                  .as[Double]
                  .getOrElse(0.0)
                val stdErr = bench.hcursor
                  .downField("primaryMetric")
                  .downField("scoreError")
                  .as[Double]
                  .getOrElse(0.0)
                val quantiles = bench.hcursor
                  .downField("primaryMetric")
                  .downField("scorePercentiles")
                  .as[Map[String, Double]]
                  .getOrElse(Map.empty)

                val confidence = bench.hcursor
                  .downField("primaryMetric")
                  .downField("scoreConfidence")
                  .as[Array[Double]]
                  .getOrElse(Array.empty)

                quantiles.foldLeft(
                  config
                    .withConfigValue(s"benchmarks.$name.metric", f"$metric%.4f")
                    .withConfigValue(s"benchmarks.$name.measurements", measurements)
                    .withConfigValue(
                      s"benchmarks.$name.ciLeft",
                      f"${Try(confidence(0)).getOrElse(0.0)}%.4f")
                    .withConfigValue(
                      s"benchmarks.$name.ciRight",
                      f"${Try(confidence(1)).getOrElse(0.0)}%.4f")
                    .withConfigValue(s"benchmarks.$name.stdErr", f"$stdErr%.4f")) {
                  (conf, quantile) =>
                    {
                      conf
                        .withConfigValue(
                          s"benchmarks.$name.quantiles.${quantile._1}",
                          f"${quantile._2}.4f")
                    }
                }
              }
            })
      }
    }.toOption.flatten.getOrElse(baseConfig)
  }

  def heliumTheme: String => ThemeProvider = (v: String) => {
    Helium.defaults.all
      .metadata(
        title = Some("GraphFrames"),
        description = Some(gfDescription),
        version = Some(v),
        language = Some("en"))
      .all
      .tableOfContent("Table of Content", depth = 4)
      .site
      .topNavigationBar(navLinks = Seq(
        IconLink.external("https://github.com/graphframes/graphframes", HeliumIcon.github),
        IconLink.external(
          "https://discord.com/channels/1162999022819225631/1326257052368113674",
          HeliumIcon.chat)))
      .site
      .landingPage(
        title = Some("GraphFrames"),
        subtitle = Some("Distributed graph processing on top of Apache Spark"),
        logo = Some(Image.internal(Root / "img" / "logo-dark.png")),
        latestReleases = Seq(
          ReleaseInfo(
            "Latest Stable Release",
            v match {
              case thisVersionShortRegex(v, _) => v
              case v => v
            })),
        license = Some("Apache-2.0"),
        linkPanel = Some(
          LinkPanel(
            title = "Documentation",
            TextLink.internal(Root / "01-about" / "01-index.md", "About GraphFrames"),
            TextLink.internal(Root / "02-quick-start" / "01-installation.md", "Quick Start"),
            TextLink.internal(Root / "03-tutorials" / "01-tutorials.md", "Tutorials"),
            TextLink
              .internal(Root / "04-user-guide" / "01-creating-graphframes.md", "User Guide"),
            TextLink.internal(Root / "05-blog" / "01-index.md", "Blog"),
            TextLink
              .internal(Root / "06-contributing" / "01-contributing-guide.md", "Contributing"),
            TextLink.internal(Root / "api" / "scaladoc" / "index.html", "API (Scaladoc)"),
            TextLink.internal(Root / "api" / "python" / "index.html", "API (Python)"))),
        projectLinks = Seq(
          TextLink.external("https://github.com/graphframes/graphframes", "GitHub"),
          TextLink.external(
            "https://discord.com/channels/1162999022819225631/1326257052368113674",
            "Discord")),
        teasers = Seq(
          Teaser(
            "Scalable",
            "Fully distributed algorithms. GraphFrames scales as well as your Spark Cluster is scaling"),
          Teaser(
            "Property Graphs Model",
            "Support flexible definition of property graphs with groups and support graphs with multiple kind of vertices and edges"),
          Teaser(
            "Feature reach",
            "A lot of ready to use graph algorithms, like PageRank and Label Propagation"),
          Teaser(
            "Mature",
            "Project is old enough already and used in a lot of places as part of another libraries and platforms or as a tool for graph processing by end users"),
          Teaser(
            "PySpark support",
            "Not only scala, but also Python with a full support of Spark Connect protocol")))
      .build
  }

  def copyAll(targetDir: Path, sourceDir: Path): Unit = {
    if (!Files.exists(sourceDir)) {
      println(s"Directory $sourceDir does not exist. Skipping.")
      return
    }
    val directoryConf = targetDir.resolve("directory.conf")

    if (Files.exists(targetDir)) {
      println(
        s"Removing files in the existing documentation directory: ${targetDir.toAbsolutePath}")
      Files
        .walk(targetDir)
        .sorted(java.util.Comparator.reverseOrder())
        .filter(!_.equals(targetDir))
        .filter(!_.equals(directoryConf))
        .forEach(f => Files.delete(f))
    }

    println(
      s"Copying files of the documentation from ${sourceDir.toAbsolutePath} to ${targetDir.toAbsolutePath}")
    Files
      .walk(sourceDir)
      .forEach { source =>
        val target = targetDir.resolve(sourceDir.relativize(source))
        if (Files.isDirectory(source)) {
          Files.createDirectories(target)
        } else {
          Files.createDirectories(target.getParent)
          Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING)
        }
      }
  }
}
