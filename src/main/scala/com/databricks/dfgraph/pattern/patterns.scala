package com.databricks.dfgraph.pattern

import scala.util.parsing.combinator._

/**
 * Parser for graph patterns. Copied from GraphFrames with minor modification.
 */
object PatternParser extends RegexParsers {
  private val vertexName: Parser[Vertex] = "[a-zA-Z0-9_]+".r ^^ { NamedVertex(_) }
  private val anonymousVertex: Parser[Vertex] = "" ^^ { x => AnonymousVertex }
  private val vertex: Parser[Vertex] = "(" ~> (vertexName | anonymousVertex) <~ ")"
  private val namedEdge: Parser[Edge] =
    vertex ~ "-" ~ "[" ~ "[a-zA-Z0-9_]+".r ~ "]" ~ "->" ~ vertex ^^ {
      case src ~ "-" ~ "[" ~ name ~ "]" ~ "->" ~ dst => NamedEdge(name, src, dst)
    }
  val anonymousEdge: Parser[Edge] =
    vertex ~ "-" ~ "[" ~ "]" ~ "->" ~ vertex ^^ {
      case src ~ "-" ~ "[" ~ "]" ~ "->" ~ dst => AnonymousEdge(src, dst)
    }
  private val edge: Parser[Edge] = namedEdge | anonymousEdge
  private val negatedEdge: Parser[Pattern] =
    "!" ~ edge ^^ {
      case "!" ~ e => Negation(e)
    }
  private val pattern: Parser[Pattern] = edge | vertex | negatedEdge
  val patterns: Parser[List[Pattern]] = repsep(pattern, ";")
}

object Pattern {
  def parse(s: String): Seq[Pattern] = {
    import PatternParser._
    parseAll(patterns, s).get
  }
}

sealed trait Pattern

case class Negation(child: Edge) extends Pattern

sealed trait Vertex extends Pattern

case object AnonymousVertex extends Vertex

case class NamedVertex(name: String) extends Vertex

sealed trait Edge extends Pattern

case class AnonymousEdge(src: Vertex, dst: Vertex) extends Edge

case class NamedEdge(name: String, src: Vertex, dst: Vertex) extends Edge
