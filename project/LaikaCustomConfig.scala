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
import scala.util.matching.Regex

object LaikaCustomConfig {
  val gfDescription: String = "Scalable Graph Processing on top of Apache Spark"
  val thisVersionShortRegex: Regex = """^([0-9]+\.[0-9]+\.[0-9]+)(.*)$""".r
  val laikaConfig: LaikaConfig = LaikaConfig.defaults.withRawContent
    .withConfigValue(LaikaKeys.site.apiPath, "api/scaladoc")
    .withConfigValue(LinkConfig.empty.addSourceLinks(
      SourceLinks(baseUri = "https://github.com/graphframes/graphframes", suffix = "scala")))
    .withConfigValue(LinkConfig.empty.addSourceLinks(
      SourceLinks(baseUri = "https://github.com/graphframes/graphframes", suffix = "py")))

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
