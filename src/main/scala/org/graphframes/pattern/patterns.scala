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

package org.graphframes.pattern

import scala.collection.mutable
import scala.util.parsing.combinator._

import org.graphframes.InvalidParseException

/**
 * Parser for graph patterns for motif finding. Copied from GraphFrames with minor modification.
 */
private[graphframes] object PatternParser extends RegexParsers {
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

private[graphframes] object Pattern {
  def parse(s: String): Seq[Pattern] = {
    import PatternParser._
    val result = parseAll(patterns, s) match {
      case result: Success[_] =>
        result.asInstanceOf[Success[Seq[Pattern]]].get
      case result: NoSuccess =>
        throw new InvalidParseException(
          s"Failed to parse bad motif string: '$s'.  Returned message: ${result.msg}")
    }
    assertValidPatterns(result)
    result
  }

  /**
   * Checks all Patterns for validity:
   *  - Disallow named edges within negated terms
   *  - Disallow term "()-[]->()" and its negation
   *  - Disallow name to be shared by a vertex and an edge
   * @throws InvalidParseException if an negated terms contain named edges
   */
  private def assertValidPatterns(patterns: Seq[Pattern]): Unit = {

    // vertexNames, edgeNames are used to check for duplicate names across vertices and edges
    val vertexNames = mutable.HashSet.empty[String]
    val edgeNames = mutable.HashSet.empty[String]
    def addVertex(v: Vertex): Unit = v match {
      case NamedVertex(name) =>
        if (edgeNames.contains(name)) {
          throw new InvalidParseException(s"Motif reused name '$name' for both a vertex and " +
            s"an edge, which is not allowed.")
        }
        vertexNames += name
      case AnonymousVertex =>  // pass
    }
    def addEdge(e: Edge): Unit = e match {
      case NamedEdge(name, src, dst) =>
        if (vertexNames.contains(name)) {
          throw new InvalidParseException(s"Motif reused name '$name' for both a vertex and " +
            s"an edge, which is not allowed.")
        }
        if (edgeNames.contains(name)) {
          throw new InvalidParseException(s"Motif reused name '$name' for multiple edges, " +
            s"which is not allowed.")
        }
        edgeNames += name
        addVertex(src)
        addVertex(dst)
      case AnonymousEdge(src, dst) =>
        addVertex(src)
        addVertex(dst)
    }

    patterns.foreach {
      case Negation(edge) =>
        edge match {
          case NamedEdge(name, src, dst) =>
            throw new InvalidParseException(s"Motif finding does not support negated named " +
              s"edges, but the given pattern contained: !($src)-[$name]->($dst)")
          case AnonymousEdge(AnonymousVertex, AnonymousVertex) =>
            throw new InvalidParseException(s"Motif finding does not support completely " +
              s"anonymous negated edges !()-[]->().  Users can check for 0 edges in the graph " +
              s"using the edges DataFrame.")
          case e @ AnonymousEdge(_, _) =>
            addEdge(e)
        }
      case AnonymousEdge(AnonymousVertex, AnonymousVertex) =>
        throw new InvalidParseException(s"Motif finding does not support completely " +
          s"anonymous edges ()-[]->().  Users can check for the existence of edges in the " +
          s"graph using the edges DataFrame.")
      case e @ AnonymousEdge(_, _) =>
        addEdge(e)
      case e @ NamedEdge(_, _, _) =>
        addEdge(e)
      case AnonymousVertex =>
        throw new InvalidParseException("Motif finding does not allow a lone anonymous vertex " +
          "\"()\" in a motif.  Users can check for the existence of vertices in the graph " +
          "using the vertices DataFrame.")
      case v @ NamedVertex(_) =>
        addVertex(v)
    }
  }

  /**
   * Return the set of named vertices which only appear in negated terms, in sorted order.
   */
  private[graphframes]
  def findNamedVerticesOnlyInNegatedTerms(patterns: Seq[Pattern]): Seq[String] = {
    val vPos = findNamedElementsInOrder(
      patterns.filter(p => !p.isInstanceOf[Negation]), includeEdges = false).toSet
    val vNeg = findNamedElementsInOrder(
      patterns.filter(p => p.isInstanceOf[Negation]), includeEdges = false).toSet
    vNeg.diff(vPos).toSeq.sorted
  }

  /**
   * Return the set of named vertices (and optionally edges) appearing in the given patterns,
   * in the order they first appear in the sequence of patterns.
   * @param includeEdges If true, include named edges in the returned sequence.
   */
  private[graphframes]
  def findNamedElementsInOrder(patterns: Seq[Pattern], includeEdges: Boolean): Seq[String] = {
    val elementSet = mutable.LinkedHashSet.empty[String]
    def findNamedElementsHelper(pattern: Pattern): Unit = pattern match {
      case Negation(child) =>
        findNamedElementsHelper(child)
      case AnonymousVertex =>  // pass
      case NamedVertex(name) =>
        if (!elementSet.contains(name)) {
          elementSet += name
        }
      case AnonymousEdge(src, dst) =>
        findNamedElementsHelper(src)
        findNamedElementsHelper(dst)
      case NamedEdge(name, src, dst) =>
        findNamedElementsHelper(src)
        if (includeEdges && !elementSet.contains(name)) {
          elementSet += name
        }
        findNamedElementsHelper(dst)
    }
    patterns.foreach(findNamedElementsHelper)
    elementSet.toSeq
  }
}

private[graphframes] sealed trait Pattern

private[graphframes] case class Negation(child: Edge) extends Pattern

private[graphframes] sealed trait Vertex extends Pattern

private[graphframes] case object AnonymousVertex extends Vertex

private[graphframes] case class NamedVertex(name: String) extends Vertex

private[graphframes] sealed trait Edge extends Pattern

private[graphframes] case class AnonymousEdge(src: Vertex, dst: Vertex) extends Edge

private[graphframes] case class NamedEdge(name: String, src: Vertex, dst: Vertex) extends Edge

