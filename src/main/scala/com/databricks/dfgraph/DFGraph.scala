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

package com.databricks.dfgraph

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions._

import com.databricks.dfgraph.pattern._

/**
 * Represents a [[Graph]] with vertices and edges stored as [[DataFrame]]s.
 * [[vertices]] must contain a column named "id" that stores unique vertex IDs.
 * [[edges]] must contain two columns "src" and "dst" that store source vertex IDs and target
 * vertex IDs of edges, respectively.
 *
 * @param vertices the [[DataFrame]] holding vertex information
 * @param edges the [[DataFrame]] holding edge information
 * @param idCol Column name for vertex IDs in [[vertices]]
 * @param srcCol Column name for source vertex IDs in [[edges]]
 * @param dstCol Column name for destination vertex IDs in [[edges]]
 */
class DFGraph protected (
    @transient val vertices: DataFrame,
    @transient val edges: DataFrame,
    val idCol: String,
    val srcCol: String,
    val dstCol: String) extends Serializable {

  require(vertices.columns.contains(idCol),
    s"Vertex ID column '$idCol' missing from vertex DataFrame.")
  require(edges.columns.contains(srcCol),
    s"Source vertex ID column '$srcCol' missing from edge DataFrame.")
  require(edges.columns.contains(dstCol),
    s"Destination vertex ID column '$dstCol' missing from edge DataFrame.")

  import DFGraph._

  private def sqlContext: SQLContext = vertices.sqlContext

  /** Default constructor is provided to support serialization */
  protected def this() = this(null, null, DFGraph.ID, DFGraph.SRC_ID, DFGraph.DST_ID)

  // ============================ Motif finding ========================================

  /**
   * Motif finding.
   * TODO: Describe possible motifs.
   * @param pattern  Pattern specifying a motif to search for.
   * @return  [[DataFrame]] containing all instances of the motif.
   *          TODO: Describe column naming patterns.
   */
  def find(pattern: String): DataFrame =
    findSimple(Nil, None, Pattern.parse(pattern))

  /**
   * Primary method implementing motif finding.
   * This recursive method handles one pattern (via [[findIncremental()]] on each iteration,
   * augmenting the [[DataFrame]] in prevDF with each new pattern.
   *
   * @param prevPatterns  Patterns already handled
   * @param prevDF  Current DataFrame based on prevPatterns
   * @param remainingPatterns  Patterns not yet handled
   * @return  [[DataFrame]] augmented with the next pattern, or the previous DataFrame if done
   */
  private def findSimple(
      prevPatterns: Seq[Pattern],
      prevDF: Option[DataFrame],
      remainingPatterns: Seq[Pattern]): DataFrame = {
    remainingPatterns match {
      case Nil => prevDF.getOrElse(sqlContext.emptyDataFrame)
      case cur :: rest =>
        val df = findIncremental(prevPatterns, prevDF, cur)
        findSimple(prevPatterns :+ cur, df, rest)
    }
  }

  // Helper methods defining column naming conventions for motif finding
  private def prefixWithName(name: String, col: String): String = name + "_" + col
  private def vId(name: String): String = prefixWithName(name, "id")
  private def eSrcId(name: String): String = prefixWithName(name, "src")
  private def eDstId(name: String): String = prefixWithName(name, "dst")
  private def pfxE(name: String): DataFrame = renameAll(edges, prefixWithName(name, _))
  private def pfxV(name: String): DataFrame = renameAll(vertices, prefixWithName(name, _))

  private def maybeJoin(aOpt: Option[DataFrame], b: DataFrame): DataFrame = {
    aOpt match {
      case Some(a) => a.join(b)
      case None => b
    }
  }

  private def maybeJoin(
      aOpt: Option[DataFrame],
      b: DataFrame,
      joinExprs: DataFrame => Column): DataFrame = {
    aOpt match {
      case Some(a) => a.join(b, joinExprs(a))
      case None => b
    }
  }

  /** Indicate whether a named vertex has been seen in any of the given patterns */
  private def seen(v: NamedVertex, patterns: Seq[Pattern]) = patterns.exists(p => seen1(v, p))

  /** Indicate whether a named vertex has been seen in the given pattern */
  private def seen1(v: NamedVertex, pattern: Pattern): Boolean = pattern match {
    case Negation(edge) =>
      seen1(v, edge)
    case AnonymousEdge(src, dst) =>
      seen1(v, src) || seen1(v, dst)
    case NamedEdge(_, src, dst) =>
      seen1(v, src) || seen1(v, dst)
    case v2 @ NamedVertex(_) =>
      v2 == v
    case AnonymousVertex =>
      false
  }

  /**
   * Augment the given DataFrame based on a pattern.
   * @param prevPatterns  Patterns which have contributed to the given DataFrame
   * @param prev  Given DataFrame
   * @param pattern  Pattern to search for
   * @return  DataFrame augmented with the current search pattern
   */
  private def findIncremental(
      prevPatterns: Seq[Pattern],
      prev: Option[DataFrame],
      pattern: Pattern): Option[DataFrame] = pattern match {

    case AnonymousVertex =>
      prev

    case v @ NamedVertex(name) =>
      if (seen(v, prevPatterns)) {
        for (prev <- prev) assert(prev.columns.toSet.contains(vId(name)))
        prev
      } else {
        Some(maybeJoin(prev, pfxV(name)))
      }

    case NamedEdge(name, AnonymousVertex, AnonymousVertex) =>
      val eRen = pfxE(name)
      Some(maybeJoin(prev, eRen))

    case NamedEdge(name, AnonymousVertex, dst @ NamedVertex(dstName)) =>
      if (seen(dst, prevPatterns)) {
        val eRen = pfxE(name)
        Some(maybeJoin(prev, eRen, prev => eRen(eDstId(name)) === prev(vId(dstName))))
      } else {
        val eRen = pfxE(name)
        val dstV = pfxV(dstName)
        Some(maybeJoin(prev, eRen)
          .join(dstV, eRen(eDstId(name)) === dstV(vId(dstName)), "left_outer"))
      }

    case NamedEdge(name, src @ NamedVertex(srcName), AnonymousVertex) =>
      if (seen(src, prevPatterns)) {
        val eRen = pfxE(name)
        Some(maybeJoin(prev, eRen, prev => eRen(eSrcId(name)) === prev(vId(srcName))))
      } else {
        val eRen = pfxE(name)
        val srcV = pfxV(srcName)
        Some(maybeJoin(prev, eRen)
          .join(srcV, eRen(eSrcId(name)) === srcV(vId(srcName))))
      }

    case NamedEdge(name, src @ NamedVertex(srcName), dst @ NamedVertex(dstName)) =>
      (seen(src, prevPatterns), seen(dst, prevPatterns)) match {
        case (true, true) =>
          val eRen = pfxE(name)
          Some(maybeJoin(prev, eRen, prev =>
            eRen(eSrcId(name)) === prev(vId(srcName)) && eRen(eDstId(name)) === prev(vId(dstName))))

        case (true, false) =>
          val eRen = pfxE(name)
          val dstV = pfxV(dstName)
          Some(maybeJoin(prev, eRen, prev => eRen(eSrcId(name)) === prev(vId(srcName)))
            .join(dstV, eRen(eDstId(name)) === dstV(vId(dstName))))

        case (false, true) =>
          val eRen = pfxE(name)
          val srcV = pfxV(srcName)
          Some(maybeJoin(prev, eRen, prev => eRen(eDstId(name)) === prev(vId(dstName)))
            .join(srcV, eRen(eSrcId(name)) === srcV(vId(srcName))))

        case (false, false) =>
          val eRen = pfxE(name)
          val srcV = pfxV(srcName)
          val dstV = pfxV(dstName)
          Some(maybeJoin(prev, eRen)
            .join(srcV, eRen(eSrcId(name)) === srcV(vId(srcName)))
            .join(dstV, eRen(eDstId(name)) === dstV(vId(dstName))))
        // TODO: expose the plans from joining these in the opposite order
      }

    case AnonymousEdge(src, dst) =>
      val tmpName = "__tmp"
      val result = findIncremental(prevPatterns, prev, NamedEdge(tmpName, src, dst))
      result.map(dropAll(_, edges.columns.map(col => prefixWithName(tmpName, col))))

    case Negation(edge) => prev match {
      case Some(p) => findIncremental(prevPatterns, Some(p), edge).map(result => p.except(result))
      case None => throw new InvalidPatternException
    }
  }

  // ============================ Conversions ========================================

  /**
   * Converts this [[DFGraph]] instance to a GraphX [[Graph]].
   * Vertex and edge attributes are the original rows in [[vertices]] and [[edges]], respectively.
   *
   * TODO: Handle non-Long vertex IDs.
   */
  def toGraphX: Graph[Row, Row] = {
    val vv = vertices.select(col(idCol), struct("*")).map { case Row(id: Long, attr: Row) =>
      (id, attr)
    }
    val ee = edges.select(col(srcCol), col(dstCol), struct("*")).map {
      case Row(srcId: Long, dstId: Long, attr: Row) =>
        Edge(srcId, dstId, attr)
    }
    Graph(vv, ee)
  }
}

object DFGraph {

  // Default names for vertex ID, source ID, and destination ID columns.
  private val ID: String = "id"
  private val SRC_ID: String = "src"
  private val DST_ID: String = "dst"

  // val VERTICES: String = "vertices"
  // val EDGES: String = "edges"

  /** Default name for attribute columns when converting from GraphX [[Graph]] format */
  private val ATTR: String = "attr"

  // ============================ Constructors and converters =================================

  /**
   * Create a new [[DFGraph]] from vertex and edge [[DataFrame]]s.
   * @param v  Vertex DataFrame.  This must include a column "id" containing unique vertex IDs.
   *           All other columns are treated as vertex attributes.
   * @param e  Edge DataFrame.  This must include columns "src" and "dst" containing source and
   *           destination vertex IDs.  All other columns are treated as edge attributes.
   * @return  New [[DFGraph]] instance
   */
  def apply(v: DataFrame, e: DataFrame): DFGraph = {
    new DFGraph(v, e, ID, SRC_ID, DST_ID)
  }

  /**
   * Create a new [[DFGraph]] from vertex and edge [[DataFrame]]s, using user-chosen vertex ID
   * column names.
   * @param v  Vertex DataFrame.  This must include a column containing unique vertex IDs.
   *           All other columns are treated as vertex attributes.
   * @param e  Edge DataFrame.  This must include columns containing source and
   *           destination vertex IDs.  All other columns are treated as edge attributes.
   * @param idCol  Column name in vertex DataFrame for vertex IDs
   * @param srcCol  Column name in edge DataFrame for source vertex IDs
   * @param dstCol  Column name in edge DataFrame for destination vertex IDs
   * @return  New [[DFGraph]] instance
   */
  def apply(v: DataFrame, e: DataFrame, idCol: String, srcCol: String, dstCol: String): DFGraph = {
    new DFGraph(v, e, idCol, srcCol, dstCol)
  }

  /*
  // TODO: Add version with uniqueKey, foreignKey from Ankur's branch?
  def apply(v: DataFrame, e: DataFrame): DFGraph = {
    require(v.columns.contains("id"))
    require(e.columns.contains("src") && e.columns.contains("dst"))
    val vK = v.uniqueKey("id")
    vK.registerTempTable("vK")
    val eK = e.foreignKey("src", "vK.id").foreignKey("dst", "vK.id")
    new DFGraph(vK, eK)
  }
  */

  /**
   * Converts a GraphX [[Graph]] instance into a [[DFGraph]].
   *
   * This converts each [[org.apache.spark.rdd.RDD]] in the [[Graph]] to a [[DataFrame]] using
   * schema inference.
   * TODO: Add version which takes explicit schemas.
   *
   * Vertex ID column names will default to "id" for the vertex DataFrame,
   * and to "src" and "dst" for the edge DataFrame.
   */
  def fromGraphX[VD : TypeTag, ED : TypeTag](graph: Graph[VD, ED]): DFGraph = {
    val sqlContext = SQLContext.getOrCreate(graph.vertices.context)
    val vv = sqlContext.createDataFrame(graph.vertices).toDF(ID, ATTR)
    val ee = sqlContext.createDataFrame(graph.edges).toDF(SRC_ID, DST_ID, ATTR)
    DFGraph(vv, ee)
  }

  // ============================ DataFrame utilities ========================================

  /** Drop all given columns from the DataFrame */
  private def dropAll(df: DataFrame, columns: Seq[String]): DataFrame =
    columns.foldLeft(df) { (df, col) => df.drop(col) }

  /** Rename all columns within a DataFrame using the given method */
  private def renameAll(df: DataFrame, f: String => String): DataFrame = {
    val colNames = df.schema.map { field =>
      val name = field.name
      new Column(name).as(f(name))
    }
    df.select(colNames : _*)
  }
}

// TODO: Make this public?  Or catch somewhere?
private class InvalidPatternException() extends Exception()
