import cats.syntax.all.*
import laika.api.bundle.BlockDirectives
import laika.api.bundle.DirectiveRegistry
import laika.api.bundle.LinkDirectives
import laika.api.bundle.SpanDirectives
import laika.api.bundle.SpanDirectives.dsl.*
import laika.api.bundle.TemplateDirectives
import laika.ast.*

object LaikaCustomDirectives extends DirectiveRegistry {
  val pydocDirective: SpanDirectives.Directive = SpanDirectives.create("pydoc") {
    (attribute(0).as[String], cursor, source).mapN { (pyClass, cursor, source) =>
      cursor.config
        .get[String]("pydoc.baseUri")
        .fold(
          error => InvalidSpan(s"Invalid PyDocs baseUri: $error", source),
          baseUri => {
            val companion = if (pyClass.startsWith("graphframes.lib")) {
              SpanLink.external(s"$baseUri/graphframes/lib.html#$pyClass")
            } else if (pyClass.startsWith("graphframes.examples")) {
              SpanLink.external(s"$baseUri/graphframes/examples.html#$pyClass")
            } else {
              SpanLink.external(s"$baseUri/graphframes.html#$pyClass")
            }

            companion(pyClass)
          })
    }
  }
  val scalaDirective: SpanDirectives.Directive = SpanDirectives.create("scaladoc") {
    (attribute(0).as[String], cursor, source).mapN { (className, cursor, source) =>
      cursor.config
        .get[String]("scaladoc.baseUri")
        .fold(
          error => InvalidSpan(s"Invalid Scaladoc baseUri: $error", source),
          baseUri => {
            SpanLink.external(s"$baseUri/${className.replace(".", "/")}.html")(className)
          })
    }
  }
  val sourceCodeLinkDirective: SpanDirectives.Directive = SpanDirectives.create("srcLink") {
    (attribute(0), cursor, source).mapN { (path, cursor, source) =>
      cursor.config
        .get[String]("github.baseUri")
        .fold(
          error => InvalidSpan(s"Invalid GitHub baseUri: $error", source),
          baseUri => SpanLink.external(s"$baseUri/blob/main/$path")(""))
    }
  }
  val spanDirectives = Seq(pydocDirective, scalaDirective, sourceCodeLinkDirective)
  val blockDirectives: Seq[BlockDirectives.Directive] = Seq()
  val templateDirectives: Seq[TemplateDirectives.Directive] = Seq()
  val linkDirectives: Seq[LinkDirectives.Directive] = Seq()
}
