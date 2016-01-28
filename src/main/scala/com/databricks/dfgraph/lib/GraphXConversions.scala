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

package com.databricks.dfgraph.lib

import scala.reflect.runtime.universe._

import org.apache.spark.graphx.Graph
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

import com.databricks.dfgraph.DFGraph

/**
 * Convenience functions to map graphX graphs to DFGraphs, checking for the types expected by GraphX.
 */
private[dfgraph] object GraphXConversions {

  import DFGraph._

  /**
   * Takes a graphx structure and merges it with the corresponding subset of a graph.
   * @param originalGraph
   * @param graph
   * @param vertexNames
   * @param edgeName
   * @tparam V the type of the vertex data
   * @tparam E the type of the edge data
   * @return
   */
  def fromGraphX[V : TypeTag, E : TypeTag](
      originalGraph: DFGraph,
      graph: Graph[V, E],
      vertexNames: Seq[String] = Nil,
      edgeName: Seq[String] = Nil): DFGraph = {
    val sqlContext = SQLContext.getOrCreate(graph.vertices.context)
    // catalyst does not like the unit type, make sure to filter it first.
    val (emptyVertex: Boolean, productVertex: Boolean) = {
      val t = typeOf[V]
      val b1 = typeOf[Unit] =:= t
      // See http://stackoverflow.com/questions/21209006/how-to-check-if-reflected-type-represents-a-tuple
      val b2 = t.typeSymbol.fullName.startsWith("scala.Tuple")
      System.err.println(s"Type of vertex is $t: ${t.typeSymbol.fullName} empty = $b1, product = $b2")
      (b1, b2)
    }
    val vertexDF: DataFrame = if (emptyVertex) {
      val vertexData = graph.vertices.map { case (vid, data) => Tuple1(vid) }
      sqlContext.createDataFrame(vertexData).toDF(LONG_ID)
    } else if (productVertex) {
      val vertexData = graph.vertices.map { case (vid, data) => (vid, data) }
      val vertexDF0 = sqlContext.createDataFrame(vertexData).toDF(LONG_ID, GX_ATTR)
      renameStructFields(vertexDF0, GX_ATTR, vertexNames)
    } else {
      // Assume it is just one field, and pack it in a tuple to have a structure.
      val vertexData = graph.vertices.map { case (vid, data) =>  (vid, Tuple1(data)) }
      val vertexDF0 = sqlContext.createDataFrame(vertexData).toDF(LONG_ID, GX_ATTR)
      renameStructFields(vertexDF0, GX_ATTR, vertexNames)
    }


    val (emptyEdge: Boolean, productEdge: Boolean) = {
      val t = typeOf[E]
      val b1 = typeOf[Unit] =:= t
      // See http://stackoverflow.com/questions/21209006/how-to-check-if-reflected-type-represents-a-tuple
      val b2 = t.typeSymbol.fullName.startsWith("scala.Tuple")
      System.err.println(s"Type of edge is $t: ${t.typeSymbol.fullName} empty = $b1, product = $b2")
      (b1, b2)
    }
    val edgeDF: DataFrame = if (emptyEdge) {
      val edgeData = graph.edges.map { e => (e.srcId, e.dstId) }
      sqlContext.createDataFrame(edgeData).toDF(LONG_SRC, LONG_DST)
    } else if (productEdge) {
      val edgeData = graph.edges.map { e => (e.srcId, e.dstId, e.attr) }
      val edgeDF0 = sqlContext.createDataFrame(edgeData).toDF(LONG_SRC, LONG_DST, GX_ATTR)
      renameStructFields(edgeDF0, GX_ATTR, edgeName)
    } else {
      val edgeData = graph.edges.map { e => (e.srcId, e.dstId, Tuple1(e.attr)) }
      val edgeDF0 = sqlContext.createDataFrame(edgeData).toDF(LONG_SRC, LONG_DST, GX_ATTR)
      renameStructFields(edgeDF0, GX_ATTR, edgeName)
    }
    fromGraphX(originalGraph, vertexDF, edgeDF)
  }

  /**
   * Given the name of a column (assumed to contain a struct), renames all the fields of this struct.
   * @param df
   * @param structName
   * @param fieldNames
   * @return
   */
  private def renameStructFields(df: DataFrame, structName: String, fieldNames: Seq[String]): DataFrame = {
    System.err.println(s"renameStructFields: df: $structName -> $fieldNames")
    df.printSchema()
    // It decompacts the struct fields into extra columns and prefixes all the other columns to make sure there is no
    // collision.
    // TODO(tjh) this looses metadata and other info in the process
    val prefix = "RENAME_STRUCT_"
    val cols = df.schema.flatMap {
      case StructField(fname, dt: StructType, nullable, meta) if fname == structName =>
        assert(dt.length == fieldNames.length, (fname, dt, fieldNames, df.schema))
        dt.iterator.toSeq.zip(fieldNames).map { case (sub, n) =>
          df(s"$fname.${sub.name}").as(n)
        }
      case f => Seq(df(f.name).as(prefix + f.name))
    }
    val unpacked = df.select(cols: _*)
    System.err.println(s"renameStructFields: unpacked:")
    unpacked.printSchema()
    val (others, groupNames) = unpacked.schema.map(_.name).partition(_.startsWith(prefix))
    val str = struct(groupNames.map(n => col(n)): _*).as(structName)
    val rest = others.map(n => col(n).as(n.stripPrefix(prefix)))
    val res = unpacked.select((rest :+ str): _*)
    System.err.println(s"renameStructFields: res:")
    res.printSchema()
    res
  }

  private def drop(df: DataFrame, cols: String*): DataFrame = {
    val remainingCols = df.schema.map(_.name).filterNot(cols.contains).map(n => df(n))
    df.select(remainingCols: _*)
  }


  private def unpackStructFields(df: DataFrame): DataFrame = {
    val cols = df.schema.flatMap {
      case StructField(fname, dt: StructType, nullable, meta) =>
        dt.iterator.map(sub => col(s"$fname.${sub.name}").as(sub.name.stripPrefix(fname)))
      case f => Seq(col(f.name))
    }
    df.select(cols: _*)
  }

  // Joins all the data from the original columns against the new data. Assumes the columns are not going to conflict.
  private def fromGraphX(originalGraph: DFGraph, gxVertexData: DataFrame, gxEdgeData: DataFrame): DFGraph = {
    System.err.println(s"fromGraphX: gxVertexData:")
    gxVertexData.printSchema()
    System.err.println(s"fromGraphX: indexedVertices:")
    originalGraph.indexedVertices.printSchema()
    // The ID is going to be unpacked from the attr field
    val packedVertices = drop(originalGraph.indexedVertices, ID).join(gxVertexData, LONG_ID)
    val vertexDF = unpackStructFields(drop(packedVertices, LONG_ID))
    System.err.println(s"fromGraphX: vertexDF:")
    vertexDF.printSchema()

    System.err.println(s"fromGraphX: indexedEdges:")
    originalGraph.indexedEdges.printSchema()
    val packedEdges = {
      val indexedEdges = originalGraph.indexedEdges
      // No need to do that for the original attributes, they contain at least the vertex ids.
      val hasVertexGx = gxEdgeData.schema.exists(_.name == GX_ATTR)
      val gxCol = if (hasVertexGx) { Some(col(GX_ATTR)) } else { None }
      val sel1 = Seq(col(LONG_SRC).as("GX_LONG_SRC"), col(LONG_DST).as("GX_LONG_DST")) ++ gxCol.toSeq
      val gxe = gxEdgeData.select(sel1: _*)
      val sel3 = Seq(col(ATTR)) ++ gxCol.toSeq
      // Drop the src and dst columns from the index, they are already in the attributes and will be unpacked with
      // the rest of the user columns.
      // TODO(tjh) 2-step join?
      val join0 = gxe.join(
        indexedEdges.select(indexedEdges(LONG_SRC), indexedEdges(LONG_DST), indexedEdges(ATTR)),
        (gxe("GX_LONG_SRC") === indexedEdges(LONG_SRC)) && (gxe("GX_LONG_DST") === indexedEdges(LONG_DST)))
        .select(sel3: _*)
      join0
    }
    val edgeDF = unpackStructFields(drop(packedEdges, LONG_SRC, LONG_DST))
    System.err.println(s"fromGraphX: edgeDF:")
    edgeDF.printSchema()

    DFGraph(vertexDF, edgeDF)
  }
}

private[lib] trait Arguments {
  private[lib] def check[A](a: Option[A], name: String): A = {
    a.getOrElse(throw new IllegalArgumentException)
  }
}
