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

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.{Logging, graphx}
import org.apache.spark.graphx.{Edge, Graph, TripletFields}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.SqlParser
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import com.databricks.dfgraph.lib._
import com.databricks.dfgraph.pattern._

/**
 * Represents a graph with vertices and edges stored as [[DataFrame]]s.
 * [[vertices]] must contain a column named "id" storing unique vertex IDs.
 * [[edges]] must contain two columns "src" and "dst" storing source vertex IDs and destination
 * vertex IDs of edges, respectively.
 *
 * @param vertices the [[DataFrame]] holding vertex information
 * @param edges the [[DataFrame]] holding edge information
 */
class DFGraph protected (
    @transient val vertices: DataFrame,
    @transient val edges: DataFrame) extends Logging with Serializable {

  import DFGraph._

  require(vertices.columns.contains(ID),
    s"Vertex ID column '$ID' missing from vertex DataFrame, which has columns: "
      + vertices.columns.mkString(","))
  require(edges.columns.contains(SRC),
    s"Source vertex ID column '$SRC' missing from edge DataFrame, which has columns: "
      + edges.columns.mkString(","))
  require(edges.columns.contains(DST),
    s"Destination vertex ID column '$DST' missing from edge DataFrame, which has columns: "
      + edges.columns.mkString(","))

  private def sqlContext: SQLContext = vertices.sqlContext

  /** Default constructor is provided to support serialization */
  protected def this() = this(null, null)

  // ============================ Degree metrics =======================================

  /**
   * The out-degree of each vertex in the graph, returned as a DataFrame with two columns:
   * "id" storing vertex IDs and "outDeg" (int) storing out-degrees.
   * Note that vertices with no out-degrees are not returned in the result.
   */
  @transient lazy val outDegrees: DataFrame = {
    edges.groupBy(edges(SRC).as(ID)).agg(count("*").cast("int").as("outDeg"))
  }

  /**
   * The in-degree of each vertex in the graph, returned as a DataFame with two columns:
   * "id" storing vertex IDs and "inDeg" (int) storing out-degrees.
   * Note that vertices with no in-degrees are not returned in the result.
   */
  @transient lazy val inDegrees: DataFrame = {
    edges.groupBy(edges(DST).as(ID)).agg(count("*").cast("int").as("inDeg"))
  }

  /**
   * The degree of each vertex in the graph, returned as a DataFarme with two columns:
   * "id" storing vertex IDs and "deg" (int) storing degrees.
   * Note that vertices with no degrees are not returned in the result.
   */
  @transient lazy val degrees: DataFrame = {
    edges.select(explode(array(SRC, DST)).as(ID)).groupBy(ID).agg(count("*").cast("int").as("deg"))
  }

  /**
   * A cached conversion of this graph to the GraphX structure (using data represented in the SQL row format).
   */
  @transient lazy private[dfgraph] val cachedGraphX: Graph[Row, Row] = toGraphX

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
  private def prefixWithName(name: String, col: String): String = name + "." + col
  private def vId(name: String): String = prefixWithName(name, ID)
  private def eSrcId(name: String): String = prefixWithName(name, SRC)
  private def eDstId(name: String): String = prefixWithName(name, DST)
  private def nestE(name: String): DataFrame = edges.select(nestAsCol(edges, name))
  private def nestV(name: String): DataFrame = vertices.select(nestAsCol(vertices, name))

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
        for (prev <- prev) assert(prev.columns.toSet.contains(name))
        prev
      } else {
        Some(maybeJoin(prev, nestV(name)))
      }

    case NamedEdge(name, AnonymousVertex, AnonymousVertex) =>
      val eRen = nestE(name)
      Some(maybeJoin(prev, eRen))

    case NamedEdge(name, AnonymousVertex, dst @ NamedVertex(dstName)) =>
      if (seen(dst, prevPatterns)) {
        val eRen = nestE(name)
        Some(maybeJoin(prev, eRen, prev => eRen(eDstId(name)) === prev(vId(dstName))))
      } else {
        val eRen = nestE(name)
        val dstV = nestV(dstName)
        Some(maybeJoin(prev, eRen)
          .join(dstV, eRen(eDstId(name)) === dstV(vId(dstName)), "left_outer"))
      }

    case NamedEdge(name, src @ NamedVertex(srcName), AnonymousVertex) =>
      if (seen(src, prevPatterns)) {
        val eRen = nestE(name)
        Some(maybeJoin(prev, eRen, prev => eRen(eSrcId(name)) === prev(vId(srcName))))
      } else {
        val eRen = nestE(name)
        val srcV = nestV(srcName)
        Some(maybeJoin(prev, eRen)
          .join(srcV, eRen(eSrcId(name)) === srcV(vId(srcName))))
      }

    case NamedEdge(name, src @ NamedVertex(srcName), dst @ NamedVertex(dstName)) =>
      (seen(src, prevPatterns), seen(dst, prevPatterns)) match {
        case (true, true) =>
          val eRen = nestE(name)
          Some(maybeJoin(prev, eRen, prev =>
            eRen(eSrcId(name)) === prev(vId(srcName)) && eRen(eDstId(name)) === prev(vId(dstName))))

        case (true, false) =>
          val eRen = nestE(name)
          val dstV = nestV(dstName)
          Some(maybeJoin(prev, eRen, prev => eRen(eSrcId(name)) === prev(vId(srcName)))
            .join(dstV, eRen(eDstId(name)) === dstV(vId(dstName))))

        case (false, true) =>
          val eRen = nestE(name)
          val srcV = nestV(srcName)
          Some(maybeJoin(prev, eRen, prev => eRen(eDstId(name)) === prev(vId(dstName)))
            .join(srcV, eRen(eSrcId(name)) === srcV(vId(srcName))))

        case (false, false) =>
          val eRen = nestE(name)
          val srcV = nestV(srcName)
          val dstV = nestV(dstName)
          Some(maybeJoin(prev, eRen)
            .join(srcV, eRen(eSrcId(name)) === srcV(vId(srcName)))
            .join(dstV, eRen(eDstId(name)) === dstV(vId(dstName))))
        // TODO: expose the plans from joining these in the opposite order
      }

    case AnonymousEdge(src, dst) =>
      val tmpName = "__tmp"
      val result = findIncremental(prevPatterns, prev, NamedEdge(tmpName, src, dst))
      result.map(_.drop(tmpName))

    case Negation(edge) => prev match {
      case Some(p) => findIncremental(prevPatterns, Some(p), edge).map(result => p.except(result))
      case None => throw new InvalidPatternException
    }
  }

  // ======================== Other queries ===================================

  /**
   * Breadth-first search (BFS)
   *
   * This method returns a DataFrame of valid shortest paths from vertices matching `fromExpr`
   * to vertices matching `toExpr`.  If multiple paths are valid and have the same length,
   * the DataFrame will return one Row for each path.  If no paths are valid, the DataFrame will
   * be empty.
   * Note: "Shortest" means globally shortest path.  I.e., if the shortest path between two vertices
   * matching `fromExpr` and `toExpr` is length 5 (edges) but no path is shorter than 5, then all
   * paths returned by BFS will have length 5.
   *
   * The returned DataFrame will have the following columns:
   *  - from: start vertex of path
   *  - e[i]: edge i in the path, indexed from 0
   *  - v[i]: intermediate vertex i in the path, indexed from 0
   *  - to: end vertex of path
   * Each of these columns is a StructType whose fields are the same as the columns of [[vertices]]
   * or [[edges]].
   *
   * {{{
   *   // Search from vertex "Joe" to find the closet vertices with attribute job = CEO.
   *   bfs(col("id") === "Joe", col("job") === "CEO")
   *
   *   // If we found a path of 3 edges, each row would have schema:
   *   from | e0 | v0 | e1 | v1 | e2 | to
   * }}}
   *
   * If there are ties, then each of the equal paths will be returned as a separate Row.
   *
   * If one or more vertices match both the from and to conditions, then there is a 0-hop path.
   * The returned DataFrame will have the "from" and "to" columns (as above); however,
   * the "from" and "to" columns will be exactly the same.  There will be one row for each vertex
   * in [[vertices]] matching both `fromExpr` and `toExpr`.
   *
   * @param fromExpr  Spark SQL expression specifying valid starting vertices for the BFS.
   *                  This condition will be matched against each vertex's id or attributes.
   *                  To start from a specific vertex, this could be "id = [start vertex id]".
   *                  To start from multiple valid vertices, this can operate on vertex attributes.
   * @param toExpr  Spark SQL expression specifying valid target vertices for the BFS.
   *                This condition will be matched against each vertex's id or attributes.
   * @param maxPathLength  Limit on the length of paths.  If no valid paths of length
   *                       <= maxPathLength are found, then the BFS is terminated.
   *                       (default = 10)
   * @return  DataFrame of valid shortest paths found in the BFS
   */
  def bfs(fromExpr: Column, toExpr: Column, maxPathLength: Int): DataFrame = {
    val fromDF = vertices.filter(fromExpr)
    val toDF = vertices.filter(toExpr)
    if (fromDF.take(1).length == 0 || toDF.take(1).length == 0) {
      // Return empty DataFrame
      return sqlContext.createDataFrame(
        vertices.sqlContext.sparkContext.parallelize(Seq.empty[Row]),
        vertices.schema)
    }

    val fromEqualsToDF = fromDF.filter(toExpr)
    if (fromEqualsToDF.take(1).length > 0) {
      // from == to, so return matching vertices
      return fromEqualsToDF.select(
        nestAsCol(fromEqualsToDF, "from"), nestAsCol(fromEqualsToDF, "to"))
    }

    // We handled edge cases above, so now we do BFS.

    // Edges a->b, to be reused for each iteration
    val a2b = find("(a)-[e]->(b)")

    // We will always apply fromExpr to column "a"
    val fromAExpr = new Column(SQLHelpers.getExpr(fromExpr) transform {
      case UnresolvedAttribute(nameParts) => UnresolvedAttribute("a" +: nameParts)
    })

    // DataFrame of current search paths
    var paths: DataFrame = null

    var iter = 0
    var foundPath = false
    while (iter < maxPathLength && !foundPath) {
      val nextVertex = s"v$iter"
      val nextEdge = s"e$iter"
      // Take another step
      if (iter == 0) {
        // Note: We could avoid this special case by initializing paths with just 1 "from" column,
        // but that would create a longer lineage for the result DataFrame.
        paths = a2b.filter(fromAExpr)
          .filter(col("a.id") !== col("b.id"))  // remove self-loops
          .withColumnRenamed("a", "from").withColumnRenamed("e", nextEdge)
          .withColumnRenamed("b", nextVertex)
      } else {
        val prevVertex = s"v${iter - 1}"
        val nextLinks = a2b.withColumnRenamed("a", prevVertex).withColumnRenamed("e", nextEdge)
          .withColumnRenamed("b", nextVertex)
        paths = paths.join(nextLinks, paths(prevVertex + ".id") === nextLinks(prevVertex + ".id"))
          .drop(paths(prevVertex))
        // Make sure we are not backtracking within each path.
        // TODO: Avoid crossing paths; i.e., touch each vertex at most once.
        val previousVertexChecks = Range(0, iter)
          .map(i => paths(s"v$i.id") !== paths(nextVertex + ".id"))
          .foldLeft(lit(true))((c1, c2) => c1 && c2)
        paths = paths.filter(previousVertexChecks)
      }
      // Check if done by applying toExpr to column nextVertex
      val toVExpr = new Column(SQLHelpers.getExpr(toExpr) transform {
        case UnresolvedAttribute(nameParts) => UnresolvedAttribute(nextVertex +: nameParts)
      })
      val foundPathDF = paths.filter(toVExpr)
      if (foundPathDF.take(1).length != 0) {
        // Found path
        paths = foundPathDF.withColumnRenamed(nextVertex, "to")
        foundPath = true
      }
      iter += 1
    }
    if (foundPath) {
      logInfo(s"DFGraph.bfs found path of length $iter.")
      paths
    } else {
      logInfo(s"DFGraph.bfs failed to find a path of length <= $maxPathLength.")
      // Return empty DataFrame
      sqlContext.createDataFrame(
        vertices.sqlContext.sparkContext.parallelize(Seq.empty[Row]),
        vertices.schema)
    }
  }

  /** Version of [[bfs()]] using a default max path length of 10 */
  def bfs(fromExpr: Column, toExpr: Column): DataFrame = bfs(fromExpr, toExpr, 10)

  // ============================ Conversions ========================================

  /**
   * Converts this [[DFGraph]] instance to a GraphX [[Graph]].
   * Vertex and edge attributes are the original rows in [[vertices]] and [[edges]], respectively.
   *
   * Note that vertex (and edge) attributes include vertex IDs (and source, destination IDs)
   * in order to support non-Long vertex IDs.  If the vertex IDs are not convertible to Long values,
   * then the values are indexed in order to generate corresponding Long vertex IDs (which is an
   * expensive operation).
   *
   * The column ordering of the returned [[Graph]] vertex and edge attributes are specified by
   * [[vCols]] and [[eCols]], respectively.
   */
  def toGraphX: Graph[Row, Row] = {
    val vStruct = struct(vCols.map(col): _*)
    val eStruct = struct(eCols.map(col): _*)
    if (hasIntegralIdType) {
      val vv = vertices.select(col(ID).cast(LongType), vStruct)
        .map { case Row(id: Long, attr: Row) => (id, attr) }
      val ee = edges.select(col(SRC).cast(LongType), col(DST).cast(LongType), eStruct)
        .map { case Row(srcId: Long, dstId: Long, attr: Row) => Edge(srcId, dstId, attr) }
      Graph(vv, ee)
    } else {
      // Compute Long vertex IDs
      val vv = indexedVertices.map { case Row(long_id: Long, _, attr: Row) => (long_id, attr) }
      val ee = indexedEdges.map { case Row(_, long_src: Long, _, long_dst: Long, attr: Row) =>
        Edge(long_src, long_dst, attr)
      }
      Graph(vv, ee)
    }
  }

  /**
   * True if the id type can be cast to Long.
   *
   * This is important for performance reasons. The underlying graphx
   * implementation only deals with Long types.
   */
  private[dfgraph] lazy val hasIntegralIdType: Boolean = {
    vertices.schema(ID).dataType match {
      case _ @ (ByteType | IntegerType | LongType | ShortType) => true
      case _ => false
    }
  }

  /**
   * If the id type is not integral, it is the translation table that maps the original ids to the integral ids.
   * 
   * Columns:
   *  - $LONG_ID: the new ID
   *  - $ORIGINAL_ID: the ID provided by the user
   *  - $ATTR: all the original vertex attributes
   */
  private[dfgraph] lazy val indexedVertices: DataFrame = {
    val vStruct = struct(vCols.map(col): _*)
    if (hasIntegralIdType) {
      val indexedVertices = vertices.select(vStruct.as(ATTR))
      indexedVertices.select(col(ATTR + "." + ID).as(LONG_ID), col(ATTR + "." + ID).as(ID), col(ATTR))
    } else {
      val indexedVertices = vertices.select(monotonicallyIncreasingId().as(LONG_ID), vStruct.as(ATTR))
      indexedVertices.select(col(LONG_ID), col(ATTR + "." + ID).as(ID), col(ATTR))
    }
  }

  /**
   * Columns:
   *  - $SRC
   *  - $LONG_SRC
   *  - $DST
   *  - $LONG_DST
   *  - $ATTR
   */
  private[dfgraph] lazy val indexedEdges: DataFrame = {
    val eStruct = struct(eCols.map(col): _*)
    val packedEdges = edges.select(col(SRC), col(DST), eStruct.as(ATTR))
    val indexedSourceEdges = packedEdges
      .join(indexedVertices.select(col(LONG_ID).as(LONG_SRC), col(ID).as(SRC)), SRC)
    val indexedEdges = indexedSourceEdges.select(SRC, LONG_SRC, DST, ATTR)
      .join(indexedVertices.select(col(LONG_ID).as(LONG_DST), col(ID).as(DST)), DST)
    indexedEdges
  }

  /**
   * Helper method for [[toGraphX]] which specifies the schema of vertex attributes.
   * The vertex attributes of the returned [[Graph.vertices]] are given as a [[Row]],
   * and this method defines the column ordering in that [[Row]].
   */
  lazy val vCols: Array[String] = vertices.columns

  /**
   * Version of [[vCols]] which maps column names to indices in the Rows.
   */
  lazy val vColsMap: Map[String, Int] = vCols.zipWithIndex.toMap

  /**
   * Helper method for [[toGraphX]] which specifies the schema of edge attributes.
   * The edge attributes of the returned [[Graph.edges]] are given as a [[Row]],
   * and this method defines the column ordering in that [[Row]].
   */
  lazy val eCols: Array[String] = edges.columns

  /**
   * Version of [[eCols]] which maps column names to indices in the Rows.
   */
  lazy val eColsMap: Map[String, Int] = eCols.zipWithIndex.toMap

  /**
   *
   * Aggregates values from the neighboring edges and vertices of each vertex. The user-supplied
   * `sendMsg` function is invoked on each edge of the graph, generating 0 or more messages to be
   * sent to either vertex in the edge. The `mergeMsg` function is then used to combine all messages
   * destined to the same vertex.
   *
   * @tparam A the type of message to be sent to each vertex
   *
   * @param sendMsg runs on each edge, sending messages to neighboring vertices using the
   *   [[EdgeContext]].
   *   The first iterable collection is the collection of messages sent to the SOURCE.
   *   The second iterable collection is the collection of messages sent to the DESTINATION.
   * @param aggregate used to combine messages from `sendMsg` destined to the same vertex. This
   *   combiner should be commutative and associative.
   * @param selectedFields which fields should be included in the [[EdgeContext]] passed to the
   *   `sendMsg` function. If not all fields are needed, specifying this can improve performance.
   *
   * @example We can use this function to compute the in-degree of each
   * vertex
   * {{{
   * val rawGraph: Graph[_, _] = Graph.textFile("twittergraph")
   * val inDeg: RDD[(VertexId, Int)] =
   *   rawGraph.aggregateMessages(ctx => Seq(1) -> Seq.empty, _ + _)
   * }}}
   *
   * It returns a dataframe with the following columns:
   *  - id: the vertex ID
   *  - all the other columns that were created by using type A.
   */
  def aggregateMessages[A : ClassTag : TypeTag](
      sendMsg: EdgeContext => (Iterable[A], Iterable[A]),
      aggregate: (A, A) => A,
      selectedFields: TripletFields = TripletFields.All): DataFrame = {
    def send(ec: graphx.EdgeContext[Row, Row, A]): Unit = {
      val ec2: EdgeContext = new EdgeContextImpl(ec.srcId, ec.dstId, ec.srcAttr, ec.dstAttr, ec.attr)
      val (src, dst) = sendMsg(ec2)
      src.foreach(ec.sendToSrc)
      dst.foreach(ec.sendToDst)
    }
    val gx: RDD[(Long, A)] = cachedGraphX.aggregateMessages(send, aggregate, selectedFields)
    val df = sqlContext.createDataFrame(gx)
    dotStar(df)
  }

  // TODO(tjh) depends on Joseph's implementation here.
  private def dotStar(df: DataFrame): DataFrame = ???

  // **** Standard library ****

  def connectedComponents(): DFGraph = ConnectedComponents.run(this)

  def labelPropagation(): LabelPropagation.Builder = new LabelPropagation.Builder(this)

  def pageRank(): PageRank.Builder = new PageRank.Builder(this)

  def shortestPaths(): ShortestPaths.Builder = new ShortestPaths.Builder(this)

  def stronglyConnectedComponents(): StronglyConnectedComponents.Builder = new StronglyConnectedComponents.Builder(this)

  def svdPlusPlus(): SVDPlusPlus.Builder = new SVDPlusPlus.Builder(this)

  def triangleCounts(): DFGraph = TriangleCount.run(this)

  // TODO: Use conditional compilation to only include this (in a separate file) for Spark 1.4
  private def expr(expr: String): Column = new Column(new SqlParser().parseExpression(expr))

}


object DFGraph {

  /** Column name for vertex IDs in [[DFGraph.vertices]] */
  val ID: String = "id"

  /** Column name for source vertex IDs in [[DFGraph.edges]] */
  val SRC: String = "src"

  /** Column name for destination vertex IDs in [[DFGraph.edges]] */
  val DST: String = "dst"

  /** Default name for attribute columns when converting from GraphX [[Graph]] format */
  private[dfgraph] val ATTR: String = "attr"

  /**
   * The integral id that is used as a surrogate id when using graphX implementation
   */
  private[dfgraph] val LONG_ID: String = "new_id"

  private[dfgraph] val LONG_SRC: String = "new_src"
  private[dfgraph] val LONG_DST: String = "new_dst"
  private[dfgraph] val GX_ATTR: String = "graphx_attr"

  /**
   * The original id.
   */
  private val ORIGINAL_ID: String = "old_id"

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
    new DFGraph(v, e)
  }

  /**
   * Given an index for the edge IDs, maps the src and dest back to
   * @param indexDF
   * @param edges
   * @return
   */
  private[dfgraph] def longifyVertexDF(indexDF: DataFrame, edges: DataFrame): DataFrame = {
    ???
  }

  private[dfgraph] def unlongifyVertexDF(indexDF: DataFrame, longEdges: DataFrame): DataFrame = {
    ???
  }

  /*
  // TODO: Add version with uniqueKey, foreignKey from Ankur's branch?
  def apply(v: DataFrame, e: DataFrame): DFGraph = {
    require(v.columns.contains(ID))
    require(e.columns.contains(SRC_ID) && e.columns.contains(DST_ID))
    val vK = v.uniqueKey(ID)
    vK.registerTempTable("vK")
    val eK = e.foreignKey("src", "vK." + ID).foreignKey("dst", "vK." + ID)
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
   * Vertex ID column names will be converted to "id" for the vertex DataFrame,
   * and to "src" and "dst" for the edge DataFrame.
   */
  def fromGraphX[VD : TypeTag, ED : TypeTag](graph: Graph[VD, ED]): DFGraph = {
    val sqlContext = SQLContext.getOrCreate(graph.vertices.context)
    val vv = sqlContext.createDataFrame(graph.vertices).toDF(ID, ATTR)
    val ee = sqlContext.createDataFrame(graph.edges).toDF(SRC, DST, ATTR)
    DFGraph(vv, ee)
  }

  // ============================ DataFrame utilities ========================================

  // I'm keeping these for now since they might be useful at some point, but they should be
  // reviewed if ever used.
  /*
  /** Drop all given columns from the DataFrame */
  private def dropAll(df: DataFrame, columns: Seq[String]): DataFrame = {
    // columns.foldLeft(df) { (df, col) => df.drop(col) }
    columns.foldLeft(df) { (df, col) =>
      // This is NOT robust to columns with periods in the names.
      val splitCol = col.split("\\.")
      dropCol(df, splitCol)
    }
  }

  /**
   * Drop a column which may be nested.
   * E.g. dropping "a.src" will remove the "src" field from the struct column "a" but will
   * not drop the entire "a" column.
   */
  private def dropCol(df: DataFrame, splitCol: Array[String]): DataFrame = {
    // Identify if the column is nested.
    val col = splitCol.head
    if (splitCol.length == 1) {
      df.drop(col)
    } else {
      df.schema(col).dataType match {
        case s: StructType =>
          val colDF = df.select(s.fieldNames.map(f => df(col + "." + f)) :_*)
          colDF.show()
          val droppedDF = dropCol(colDF, splitCol.slice(1, splitCol.length))
          droppedDF.show()
          df.drop(col).withColumn(col, nestAsCol(droppedDF, col))
        case other =>
          throw new RuntimeException(s"Unknown error in DFGraph. Expected column $col to be" +
            s" StructType, but found type: $other")
      }
    }
  }
  */

  /** Helper for using [col].* in Spark 1.4.  Returns sequence of [col].[field] for all fields */
  /*
  private def colStar(df: DataFrame, col: String): Seq[String] = {
    df.schema(col).dataType match {
      case s: StructType =>
        s.fieldNames.map(f => col + "." + f)
      case other =>
        throw new RuntimeException(s"Unknown error in DFGraph. Expected column $col to be" +
          s" StructType, but found type: $other")
    }
  }
  */

  /** Nest all columns within a single StructType column with the given name */
  private def nestAsCol(df: DataFrame, name: String): Column = {
    struct(df.columns.map(c => df(c)) :_*).as(name)
  }
}

/**
 * Exception thrown when a parsed pattern for motif finding cannot be translated into a DataFrame
 * query.
 */
class InvalidPatternException() extends Exception()
