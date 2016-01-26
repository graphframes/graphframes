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

import com.databricks.dfgraph.DFGraph
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, Metadata, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
 * Convenience functions to map graphX graphs to DFGraphs, checking for the types expected by GraphX.
 */
private[dfgraph] object GraphXConversions {

  import DFGraph._

  /**
   * Takes a graph built through some graphX algorithm by transforming 'originalGraph',
   * and converts it back to a DFGraph.
   *
   * It is assumed the graph returned by graphX only stores a vertex id as the payload for the
   * vertices.
   *
   * The new graph has the following schema:
   *  - the edges have the same schema (but they may be a subset only of the original vertices)
   *  - the vertices have the following columns:
   *    - $ID: the long ID
   *    - one other column of type Long
   *
   * @param graph the graph
   * @return
   */
  def fromVertexGraphX(
      graph: Graph[VertexId, Row],
      originalGraph: DFGraph,
      vertexColumnName: String): DFGraph = {
    val gx = graph.mapVertices { case (vid: Long, vlabel: Long) => Row(vid, vlabel) }

    val vStruct = {
      val s = originalGraph.vertices.schema(DFGraph.ID)
      val gxVId = s.copy(name = LONG_ID, dataType = LongType)
      StructType(List(
        gxVId,
        gxVId.copy(name = vertexColumnName, metadata = Metadata.empty)))
    }

    fromRowGraphX(originalGraph, gx, originalGraph.edges.schema, vStruct)
  }

  /**
   * Converts the dataframes (encoded as RDDs of Row objects) and transforms them back into an [[DFGraph]], preserving
   * all the metadata attributes in the process.
   *
   * This is an internal method to act as a bridge between graphX and non-native DFGraph algorithms.
   *
   * It is assumed that the Row objects contain the necessary attributes (id, src, dst, etc.), as they are dropped from
   * the VertexRDD and the EdgeRDD.
t   *
   * @param graph the graph to convert
   * @param edgeSchema the edge schema
   * @param vertexSchema the vertex schema
   * @return an equivalent [[DFGraph]] object
   */
  def fromRowGraphX(
      originalGraph: DFGraph,
      graph: Graph[Row, Row],
      edgeSchema: StructType,
      vertexSchema: StructType): DFGraph = {
    // Because we may at least filter out some edges, we still need to filter out the edges that are not relevant
    // anymore.
    // There may be some more efficient mechanisms, but this one is the most correct.
    val eStruct = {
      val s = originalGraph.edges.schema
      val src = s(SRC)
      val dst = s(DST)
      StructType(List(
        src.copy(name = LONG_SRC, dataType = LongType),
        dst.copy(name = LONG_DST, dataType = LongType),
        StructField(GX_ATTR, s, nullable = true)))
    }

    val gx = graph.mapEdges { e => Row(e.srcId, e.dstId, e.attr) }
    fromGraphX(originalGraph, gx, eStruct, vertexSchema)
  }

  /**
   * Transforms a graphx object encoding dataframe information into a DFGraph, preserving the columns of the original
   * dataframe in the process.
   *
   * This method is used when a graphx algorithm is called to perform a graph transformation:
   * original (DFGraph) -> graph (graphx) -> new (DFGraph)
   *
   * The following invariants are respected:
   *  - the original columns of edges and vertices are carried over
   *  - new data columns added by the graphx transform are added to edges and vertices, respectively
   *  - the ids, sources and des have the same type in the original and the final graphs
   *
   * A join with the original graph may be required if the original data is dropped (invariant (a)) or if
   * the original IDs were not long integers (invariant (c)).
   *
   * @param original the original graph before transformation
   * @param graph the graph returned by graphX
   * @param edgeSchema the schema for the edges in the row objects.
   *                   It is expected to contain an $ID column of type long, and a number of
   *                   extra columns with some data, OR an $GX_ATTRS column that contains all the attributes
   *                   packed together.
   * @param vertexSchema the schema for the vertices. It is expected to contain the following columns:
   *                      - $SRC of type long
   *                      - $DST of type long
   *                      - a number of extra columns, OR an $ATTRS column that contains all the original data
   * @return
   */
  def fromGraphX(
      original: DFGraph,
      graph: Graph[Row, Row],
      edgeSchema: StructType,
      vertexSchema: StructType): DFGraph = {
    val sqlContext = SQLContext.getOrCreate(graph.vertices.context)
    // This works by packing together all the attributes submitted by graphx (GX_ATTR) in one column, and all the
    // original attributes from the DFGraph into an other column (ATTR), and at the end unpack and merge the content of the
    // two columns together (newer data has priority).

    // Packs all the attributes together from the graphX data:
    // TODO(tjh) we should bypass this step when it is not required.
    val gxVertices: DataFrame = {
      val vRows = graph.vertices.map(_._2)
      val df = sqlContext.createDataFrame(vRows, vertexSchema)
      val extraGxVCols = vertexSchema.filterNot(f => f.name == LONG_ID).map(f => col(f.name))
      val eGxStruct = struct(extraGxVCols: _*)
      val packedVertices = df.select(col(LONG_ID), eGxStruct.as(GX_ATTR))
      packedVertices
    }
    // We need to join against the original attributes.
    // TODO(tjh) we should remove the join when the id is integral already.
    val fullVertices = gxVertices.join(original.indexedVertices, LONG_ID).select(col(ID), col(ATTR), col(GX_ATTR))
    val vertexDF = destructAttributes(fullVertices)

    // Edges: some algorithms such as SVD manipulate the edges, so we also need to join and destruct the edges.

    // A dataframe containing the graphx data. If the data has already been packed, we reuse it.
    val gxEdges: DataFrame = if (isPackedEdge(edgeSchema)) {
      val eRows = graph.edges.map(_.attr)
      sqlContext.createDataFrame(eRows, edgeSchema)
    } else {
      val eRows = graph.edges.map(_.attr)
      val df = sqlContext.createDataFrame(eRows, edgeSchema)
      val extraGxECols = edgeSchema.filterNot(f => f.name == LONG_SRC || f.name == LONG_DST).map(f => col(f.name))
      val eGxStruct = struct(extraGxECols: _*)
      val packedEdges = df.select(col(LONG_SRC), col(LONG_DST), eGxStruct.as(GX_ATTR))
      packedEdges
    }

    // Join against the original edge data
    val fullEdges = {
      val indexedEdges = original.indexedEdges
      val gxe = gxEdges.select(col(LONG_SRC).as("GX_LONG_SRC"), col(LONG_DST).as("GX_LONG_DST"), col(GX_ATTR))
      // TODO(tjh) 2-step join?
      val join0 = gxe.join(indexedEdges,
        (gxe("GX_LONG_SRC") === indexedEdges(LONG_SRC)) && (gxe("GX_LONG_DST") === indexedEdges(LONG_DST)) )
        .select(col(SRC), col(DST), col(GX_ATTR), col(ATTR))
      join0
    }

    val edgeDF = destructAttributes(fullEdges)
    DFGraph(vertexDF, edgeDF)
  }

  // Checks that a dataframe is already representing packed edges.
  private def isPackedEdge(s: StructType): Boolean = {
    val fieldNames = s.map(_.name).sorted
    fieldNames == Seq(GX_ATTR, LONG_DST, LONG_SRC)
  }

  /**
   * Looks for the $ATTRS columns and deconstructs the structure in this column as top level fields.
   *
   * If the $ATTRS column is missing, or if there in field to destruct, it will return the original dataframe.
   *
   * @param df the dataframe with a column to deconstruct
   * @param skip a set of attributes to skip when deconstructing. They should NOT include the "attrs." suffix.
   * @return
   */
  @throws[Exception]("if argument names are clashing")
  private def destructAttributes(df: DataFrame): DataFrame = {
    // It first puts the new graphX attributes, and then the original attributes
    // (without the ones overwritten by graphx).
    // The mapping (in order) of the original columns to the final, flattened columns.
    // old name -> new name
    val graphxCols = df.schema.fields.flatMap {
      case StructField(name, t: StructType, _, _) if name == GX_ATTR && t.nonEmpty =>
        t.map { f =>
          val newName = f.name.stripPrefix(s"$GX_ATTR.")
          s"$GX_ATTR.${f.name}" -> newName
        }
      case _ => None
    }
    val origCols: Seq[(String, String)] = df.schema.fields.flatMap {
      case StructField(name, t: StructType, _, _) if name == ATTR && t.nonEmpty =>
        t.map { f =>
          s"$ATTR.${f.name}" -> f.name.stripPrefix(s"$ATTR.")
        }
      case _ => None
    }
    val graphxColNames = graphxCols.map { case (oldName, newName) => newName }.toSet
    val usedOrigCols = origCols.filterNot { case (oldName, newName) => graphxColNames.contains(newName) }
    val selection = (graphxCols ++ usedOrigCols).map { case (oldName, newName) => col(oldName).as(newName) } .toSeq
    df.select(selection: _*)
  }

  /**
   * The format used by graphX to carry the ID.
   * @param schema
   * @return
   */
  private[lib] def longId(schema: StructType): StructField = {
    schema(ID).copy(name = LONG_ID, dataType = LongType)
  }

}

private[lib] trait Arguments {
  private[lib] def check[A](a: Option[A], name: String): A = {
    a.getOrElse(throw new IllegalArgumentException)
  }
}
