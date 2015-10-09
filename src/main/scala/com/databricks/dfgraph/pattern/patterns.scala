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

package com.databricks.dfgraph.pattern

import scala.util.parsing.combinator._

/**
 * Parser for graph patterns for motif finding. Copied from GraphFrames with minor modification.
 */
private[dfgraph] object PatternParser extends RegexParsers {
  private val vertexName: Parser[Vertex] = "[a-zA-Z0-9_]+".r ^^ { NamedVertex }
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

private[dfgraph] object Pattern {
  def parse(s: String): Seq[Pattern] = {
    import PatternParser._
    parseAll(patterns, s) match {
      case result: Success[_] =>
        result.asInstanceOf[Success[Seq[Pattern]]].get
      case result: NoSuccess =>
        throw new InvalidParseException(
          s"Failed to parse bad motif string: '$s'.  Returned message: ${result.msg}")
    }
  }
}

private[dfgraph] sealed trait Pattern

private[dfgraph] case class Negation(child: Edge) extends Pattern

private[dfgraph] sealed trait Vertex extends Pattern

private[dfgraph] case object AnonymousVertex extends Vertex

private[dfgraph] case class NamedVertex(name: String) extends Vertex

private[dfgraph] sealed trait Edge extends Pattern

private[dfgraph] case class AnonymousEdge(src: Vertex, dst: Vertex) extends Edge

private[dfgraph] case class NamedEdge(name: String, src: Vertex, dst: Vertex) extends Edge

/**
 * Exception thrown when a pattern String for motif finding cannot be parsed.
 */
class InvalidParseException(message: String) extends Exception(message)
