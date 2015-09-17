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

import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag
import scala.util.Success
import scala.util.Try

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

object GraphFrame {
  def apply(v: DataFrame, e: DataFrame): GraphFrame = {
    require(v.columns.contains("id"))
    require(e.columns.contains("src_id") && e.columns.contains("dst_id"))
    val vK = v.uniqueKey("id")
    vK.registerTempTable("vK")
    val eK = e.foreignKey("src_id", "vK.id").foreignKey("dst_id", "vK.id")
    new GraphFrame(vK, eK)
  }
}

class GraphFrame protected (
    @transient val v: DataFrame,
    @transient val e: DataFrame)
  extends Serializable {

  def vertices = v
  def edges = e

  private val views = mutable.Map[Seq[Pattern], DataFrame]()

  private def sqlContext = v.sqlContext

  /** Default constructor is provided to support serialization */
  protected def this() = this(null, null)

  def find(pattern: String): DataFrame =
    find(pattern, identity)

  def find(pattern: String, f: DataFrame => DataFrame): DataFrame =
    // find1(Pattern.parse(pattern), f)
    f(findSimple(Nil, None, Pattern.parse(pattern)))

  private def find1(patterns: Seq[Pattern], f: DataFrame => DataFrame): DataFrame = {
    require(patterns.nonEmpty)
    val plans = mutable.Map[Seq[Pattern], Try[Option[DataFrame]]]()
    plans(Seq.empty) = Success(None)
    for {
      length <- 1 to patterns.size
      comb <- patterns.combinations(length)
      subseq <- comb.permutations
      cur = subseq.last
      prev = subseq.init
    } {
      plans(subseq) = views.get(subseq) match {
        case Some(view) => Success(Some(view))
        case None => plans(prev).flatMap(prevDF => Try(findIncremental(prev, prevDF, cur)))
      }
    }

    val finalPlans = patterns.permutations.flatMap(plans(_).toOption).flatten.map(f).toSeq
    // println(s"${finalPlans.size} plans for find($patterns):")
    // for (p <- finalPlans) println(s"${cost(p)}   ${p.queryExecution.optimizedPlan}")
    if (finalPlans.nonEmpty) finalPlans.minBy(cost) else f(sqlContext.emptyDataFrame)
  }

  private def cost(df: DataFrame): Int = df.queryExecution.optimizedPlan.collect {
    case j: Join => j
  }.size

  private def findSimple(prevPatterns: Seq[Pattern], prevDF: Option[DataFrame], remainingPatterns: Seq[Pattern]): DataFrame = {
    remainingPatterns match {
      case Nil => prevDF.getOrElse(sqlContext.emptyDataFrame)
      case cur :: rest =>
        val df = findIncremental(prevPatterns, prevDF, cur)
        findSimple(prevPatterns :+ cur, df, rest)
    }
  }

  private def prefixWithName(name: String, col: String) = name + "_" + col
  private def vId(name: String) = prefixWithName(name, "id")
  private def eSrcId(name: String) = prefixWithName(name, "src_id")
  private def eDstId(name: String) = prefixWithName(name, "dst_id")
  private def pfxE(name: String) = renameAll(edges, prefixWithName(name, _))
  private def pfxV(name: String) = renameAll(vertices, prefixWithName(name, _))

  private def maybeJoin(aOpt: Option[DataFrame], b: DataFrame): DataFrame =
    aOpt match {
      case Some(a) => a.join(b)
      case None => b
    }

  private def maybeJoin(
      aOpt: Option[DataFrame], b: DataFrame, joinExprs: DataFrame => Column): DataFrame =
    aOpt match {
      case Some(a) => a.join(b, joinExprs(a))
      case None => b
    }

  private def seen(v: NamedVertex, ps: Seq[Pattern]) = ps.exists(p => seen1(v, p))
  private def seen1(v: NamedVertex, p: Pattern): Boolean = p match {
    case Negation(edge) =>
      seen1(v, edge)
    case AnonymousEdge(src, dst) =>
      seen1(v, src) || seen1(v, dst)
    case NamedEdge(_, src, dst) =>
      seen1(v, src) || seen1(v, dst)
    case v2 @ NamedVertex(_) =>
      v2 == v
    case AnonymousVertex() =>
      false
  }

  private def findIncremental(
      prevPatterns: Seq[Pattern],
      prev: Option[DataFrame],
      p: Pattern): Option[DataFrame] = p match {

    case AnonymousVertex() =>
      prev

    case v @ NamedVertex(name) =>
      if (seen(v, prevPatterns)) {
        for (prev <- prev) assert(prev.columns.toSet.contains(vId(name)))
        prev
      } else {
        Some(maybeJoin(prev, pfxV(name)))
      }

    case NamedEdge(name, AnonymousVertex(), AnonymousVertex()) =>
      val eRen = pfxE(name)
      Some(maybeJoin(prev, eRen))

    case NamedEdge(name, AnonymousVertex(), dst @ NamedVertex(dstName)) =>
      if (seen(dst, prevPatterns)) {
        val eRen = pfxE(name)
        Some(maybeJoin(prev, eRen, prev => eRen(eDstId(name)) === prev(vId(dstName))))
      } else {
        val eRen = pfxE(name)
        val dstV = pfxV(dstName)
        Some(maybeJoin(prev, eRen)
          .join(dstV, eRen(eDstId(name)) === dstV(vId(dstName)), "left_outer"))
      }

    case NamedEdge(name, src @ NamedVertex(srcName), AnonymousVertex()) =>
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
      result.map(dropAll(_, e.columns.map(col => prefixWithName(tmpName, col))))

    case Negation(edge) => prev match {
      case Some(prev) =>
        findIncremental(prevPatterns, Some(prev), edge).map(result => prev.except(result))
      case None => throw new InvalidPatternException
    }

  }

  private def dropAll(df: DataFrame, columns: Seq[String]): DataFrame =
    columns.foldLeft(df) { (df, col) => df.drop(col) }

  private def renameAll(df: DataFrame, f: String => String): DataFrame = {
    val colNames = df.schema.map { field =>
      val name = field.name
      new Column(name).as(f(name))
    }
    df.select(colNames : _*)
  }
}

private class InvalidPatternException() extends Exception()
