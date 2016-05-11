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

package org.graphframes.lib

import scala.collection.mutable

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.SQLHelpers.zipWithUniqueId
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.graphframes.GraphFrame
import org.graphframes.GraphFrame.{ATTR, DST, ID, LONG_DST, LONG_ID, LONG_SRC, SRC}

/**
 * Connected components algorithm.
 *
 * Computes the connected component membership of each vertex and returns a DataFrame of vertex
 * information with each vertex assigned a component ID.
 *
 * The resulting DataFrame contains all the vertex information and one additional column:
 *  - component (`LongType`): unique ID for this component
 *
 * The resulting edges DataFrame is the same as the original edges DataFrame.
 */
class ConnectedComponentsDF private[graphframes] (private val graph: GraphFrame) extends Arguments {

  /**
   * Runs the algorithm.
   */
  def run(): DataFrame = {
    ConnectedComponentsDF.run(graph)
  }
}

private object ConnectedComponentsDF {

  private def cacheDataFrame(df: DataFrame): DataFrame = {
    val tmpRdd = df.rdd.cache()
    df.sqlContext.createDataFrame(tmpRdd, df.schema)
  }

  private def checkpointDataFrame(df: DataFrame): DataFrame = {
    val tmpRdd = df.rdd
    // Cache first so local checkpoint uses memory + disk.
    tmpRdd.cache()
    tmpRdd.localCheckpoint()
    df.sqlContext.createDataFrame(tmpRdd, df.schema)
  }

  // TODO: Use IntegerType when possible.
  private val ORIG_ID = "ORIG_ID"

  def zipWithUniqueIdFrom0(df: DataFrame, dataType: DataType): DataFrame = {
    val dataTypeStr = dataType.simpleString
    val sqlContext = df.sqlContext
    val schema = df.schema
    val outputSchema = StructType(Seq(
      StructField("row", schema, false), StructField("uniq_id", dataType, false)))
    val rdd = if (dataTypeStr == "long") {
      df.rdd.zipWithIndex().map { case (row: Row, id: Long) => Row(row, id) }
    } else if (dataTypeStr == "int") {
      df.rdd.zipWithIndex().map { case (row: Row, id: Long) => Row(row, id.toInt) }
    } else {
      throw new IllegalArgumentException(s"Bad vertex index type: $dataTypeStr")
    }
    sqlContext.createDataFrame(rdd, outputSchema)
  }

  /**
   * Get graph with integer-indexed vertices.
   * If possible, this will use IntegerType indices.  If there are too many vertices,
   * then this will use LongType.
   * This also removes self-edges (single-edge loops).
   *
   * @return  (indexedVertices, indexedEdges, index type)
   */
  def getIndexedGraph(graph: GraphFrame, numVertices: Long): (DataFrame, DataFrame, DataType) = {
    val dataType = if (numVertices < Int.MaxValue) DataTypes.IntegerType else DataTypes.LongType
    val dataTypeStr = dataType.simpleString

    // Special indexing in [0,...,numVertices).  This also drops the attribute columns.
    val hasIntegralIdType: Boolean = {
      graph.vertices.schema(ID).dataType match {
        case _ @ (ByteType | IntegerType | LongType | ShortType) => true
        case _ => false
      }
    }
    val indexedV0: DataFrame = {
      if (hasIntegralIdType) {
        graph.vertices.select(col(ID).cast(dataTypeStr).as(LONG_ID), col(ID).as(ID))
      } else {
        val indexedVertices = zipWithUniqueIdFrom0(graph.vertices, dataType)
        indexedVertices.select(col("uniq_id").as(LONG_ID), col("row." + ID).as(ID))
      }
    }
    val indexedE0: DataFrame = {
      val packedEdges = graph.edges.select(SRC, DST)
      val indexedSourceEdges = packedEdges
        .join(indexedV0.select(col(LONG_ID).as(LONG_SRC), col(ID).as(SRC)), SRC)
      val indexedEdges = indexedSourceEdges.select(SRC, LONG_SRC, DST)
        .join(indexedV0.select(col(LONG_ID).as(LONG_DST), col(ID).as(DST)), DST)
      indexedEdges.select(SRC, LONG_SRC, DST, LONG_DST)
    }

    val indexedVertices: DataFrame = indexedV0
      .select(col(ID).as(ORIG_ID), col(LONG_ID).as(ID), col(LONG_ID).as(COMPONENT_ID))
    val indexedEdges: DataFrame = indexedE0
      .select(col(LONG_SRC).as(SRC), col(LONG_DST).as(DST))
      .where(col(SRC) !== col(DST)) // remove self-edges
    (indexedVertices, indexedEdges, dataType)
  }

  /**
   *
   * @param graph  This should be cached beforehand.
   * @return
   */
  def run(graph: GraphFrame): DataFrame = {
  // Add initial component column
  val NEW_COMPONENT_ID = "NEW_COMPONENT_ID"
  val maxIterations = 100

  def computeNewComponents(v: DataFrame, e: DataFrame): DataFrame = {
    // Send messages: smaller component ID -> replace larger component ID
    val g = GraphFrame(v, e)
    val triplets = g.triplets
    val msgsToSrc = triplets.select(col(SRC)(ID).as(ID),
      when(col(SRC)(COMPONENT_ID) > col(DST)(COMPONENT_ID),
        col(DST)(COMPONENT_ID)).as(NEW_COMPONENT_ID))
    val msgsToDst = triplets.select(col(DST)(ID).as(ID),
      when(col(SRC)(COMPONENT_ID) < col(DST)(COMPONENT_ID),
        col(SRC)(COMPONENT_ID)).as(NEW_COMPONENT_ID))
    val msgs = msgsToSrc.unionAll(msgsToDst)
    val newComponents = msgs.groupBy(ID)
      .agg(min(NEW_COMPONENT_ID).as(NEW_COMPONENT_ID))
      .where(col(NEW_COMPONENT_ID).isNotNull)
    newComponents
  }

  val numOrigVertices = graph.vertices.count()
  val (origVertices0: DataFrame, edges0: DataFrame, idxDataType: DataType) =
    getIndexedGraph(graph, numOrigVertices)
  val origVertices: DataFrame = checkpointDataFrame(origVertices0)

  // Remove duplicate edges
  val edges1 = edges0.select(when(col(SRC) < col(DST), col(SRC)).otherwise(col(DST)).as(SRC),
    when(col(SRC) < col(DST), col(DST)).otherwise(col(SRC)).as(DST))
    .distinct
    .cache()
  // Handle high-degree vertices.
  val HANDLE_SKEW = false
  val (edges2: DataFrame, bigComponents: Map[Long, DataFrame]) = if (HANDLE_SKEW) {
    // If there is little skew, then edges2 can be the same DataFrame as edges1.
    println(s"Handling skew...")
    handleSkew(edges1, numOrigVertices)
    println(s"Done handling skew.")
  } else {
    (edges1, Map.empty[Long, DataFrame])
  }
  // Construct vertices2 from edges2.
  // This also handles vertices without edges, which will be added back in at the end.
  val vInEdges2 = edges2.select(explode(array(SRC, DST)).as(ID)).distinct
  val vertices2: DataFrame = origVertices.join(vInEdges2, ID)

  var v: DataFrame = vertices2
  val origEdges: DataFrame = checkpointDataFrame(edges2)
  var e: DataFrame = origEdges
  var iter = 0

  var lastCachedVertices: DataFrame = null
  var lastCachedEdges: DataFrame = null
  var lastCachedNewComponents: DataFrame = null

  v = checkpointDataFrame(v)
  lastCachedVertices = v

  // Send messages: smaller component ID -> replace larger component ID
  // We copy this update of components before the loop to simplify the caching logic.
  var newComponents: DataFrame = computeNewComponents(v, e)
  newComponents.cache()
  lastCachedNewComponents = newComponents
  var activeMessageCount: Long = newComponents.count()
  println(s"activeMessageCount: $activeMessageCount")

  while (iter < maxIterations && activeMessageCount > 0) {
    println(s"ITERATION $iter")
    // Update vertices with new components
    v = v.join(newComponents, v(ID) === newComponents(ID), "left_outer")
      .select(col(ORIG_ID), v(ID),
        coalesce(col(NEW_COMPONENT_ID), col(COMPONENT_ID)).as(COMPONENT_ID))

    if (iter != 0 && iter % 4 == 0) {
      v = checkpointDataFrame(v)
    } else {
      v = cacheDataFrame(v)
    }

    if (iter != 0 && iter % 2 == 0) { // % x should have x >= 2
      // % x should have x >= 2
      // Update edges so each vertex connects to its component's master vertex.
      val newEdges = v.where(col(ID) !== col(COMPONENT_ID))
        .select(col(ID).as(SRC), col(COMPONENT_ID).as(DST))
      e = origEdges.unionAll(newEdges)
      e.cache()
      if (lastCachedEdges != null) lastCachedEdges.unpersist(blocking = false)
      lastCachedEdges = e
    }

    // Send messages: smaller component ID -> replace larger component ID
    newComponents = computeNewComponents(v, e)
    newComponents.cache()
    activeMessageCount = newComponents.count()
    println(s"activeMessageCount: $activeMessageCount")

    if (lastCachedVertices != null) lastCachedVertices.unpersist(blocking = false)
    lastCachedVertices = v
    if (lastCachedNewComponents != null) lastCachedNewComponents.unpersist(blocking = false)
    lastCachedNewComponents = newComponents

    iter += 1
  }
  if (lastCachedNewComponents != null) lastCachedNewComponents.unpersist(blocking = false)
  if (lastCachedEdges != null) lastCachedEdges.unpersist(blocking = false)
  // Unpersist here instead of before loop since we could have edges1 = edges2,
  // and the link between edges2 and origEdges is unclear (need to check local checkpoint impl).
  edges1.unpersist(blocking = false)
  edges2.unpersist(blocking = false)
  origEdges.unpersist(blocking = false)

  // Handle bigComponents.
  if (bigComponents.nonEmpty) {
    println("Handling bigComponents...")
    val bigComponentReps: Set[Long] = bigComponents.keys.toSet
    val isBigRep = idxDataType match {
      case _: LongType => udf { (vid: Long) => bigComponentReps.contains(vid) }
      case _: IntegerType => udf { (vid: Int) => bigComponentReps.contains(vid.toLong) }
    }
    // bigComponentIds: representative vertex ID -> component ID
    val bigComponentIds: Map[Long, Long] =
      origVertices.where(isBigRep(col(ID))).select(ID, COMPONENT_ID).map {
        case Row(id: Long, comp: Long) => id -> comp
        case Row(id: Int, comp: Int) => id.toLong -> comp.toLong
      }.collect().toMap
    bigComponents.foreach { case (vid: Long, compVertices: DataFrame) =>
      val componentId = bigComponentIds(vid)
      val component = origVertices.join(compVertices, ID)
        .select(col(ID), col(ORIG_ID),
          lit(componentId).cast(idxDataType.simpleString).as(COMPONENT_ID))
      v = v.unionAll(component)
    }
  }
  // Handle vertices without edges.
  v = origVertices.join(v, origVertices(ID) === v(ID), "left_outer")
    .select(origVertices(ID).cast("long"), origVertices(ORIG_ID),
      coalesce(v(COMPONENT_ID), origVertices(COMPONENT_ID)).cast("long").as(COMPONENT_ID))
  // TODO: unpersist origVertices, bigComponents?
  // Join COMPONENT_ID column with original vertices.
  graph.vertices.join(v.select(col(ORIG_ID).as(ID), col(COMPONENT_ID)), ID)
  }

  /**
   * Get the highest degree vertex in the graph.
   *
   * @param e  Edges in graph
   * @param HIGH_DEGREE  Threshold for an acceptably high-degree vertex.  A vertex will be returned
   *                     only if it has degree higher than this cutoff.
   * @return Highest degree vertex, or None if no vertex has degree higher than HIGH_DEGREE.
   */
  def getHighestDegreeVertex(e: DataFrame, HIGH_DEGREE: Long): Option[Long] = {
    val degrees: DataFrame = e.select(explode(array(SRC, DST)).as(ID))
      .groupBy(ID).agg(count("*").cast("long").as("degree"))
    val highDegrees: DataFrame = degrees.where(col("degree") > HIGH_DEGREE)
      .groupBy(ID).agg(max("degree").as("degree"))
    val vArray: Array[Long] = highDegrees.select(ID, "degree").take(1).map {
      case Row(id: Long, degree: Long) => id
      case Row(id: Int, degree: Long) => id.toLong
    }
    if (vArray.nonEmpty) Some(vArray(0)) else None
  }

  /**
   * Handle extremely high-degree vertices in large graphs.
   *
   * TODO: Confirm that this works with IntegerType vertex indices.
   *
   * @param edges  Edges DataFrame.  This should ideally be cached already.
   *               This should not include any self-edges (single-edge loops).
   * @return (updated edges with columns "src" and "dst",
   *          map from representative vertices of large components -> component vertices)
   *         The edges and component DataFrames are cached.
   */
  def handleSkew(
      edges: DataFrame,
      totalVertices: Long,
      MANY_VERTICES: Long = 1e6.toLong,
      HIGH_DEGREE_FRAC: Double = 0.1): (DataFrame, Map[Long, DataFrame]) = {
    // TODO: We could also handle replicated edges, possibly using a heavy hitters alg.
    val HIGH_DEGREE: Long = (HIGH_DEGREE_FRAC * totalVertices).toLong
    println(s"HIGH_DEGREE=$HIGH_DEGREE")

    var e: DataFrame = edges.select(SRC, DST)
    var lastCachedE: DataFrame = e
    var numVertices: Long = totalVertices
    var highestDegreeVertex: Option[Long] = getHighestDegreeVertex(e, HIGH_DEGREE)
    // bigComponents: hdv (representative vertex for component) -> DataFrame of component IDs
    val bigComponents = mutable.HashMap.empty[Long, DataFrame]
    // While numVertices is large enough and there exists a vertex with very high degree, ...
    var iter: Int = 0
    while (numVertices > MANY_VERTICES && highestDegreeVertex.nonEmpty) {
      println(s"handleSkew: ITERATION $iter")
      val hdv = highestDegreeVertex.get
      // We will try to replace hdv + its neighbors with a single placeholder vertex.
      // Get 1st-degree neighbors of hdv.
      var component: DataFrame = e.where(col(SRC) === hdv || col(DST) === hdv) // neighbor
        .select(when(col(SRC) === hdv, col(DST)).otherwise(col(SRC)).as(ID)) // select neighbor
        .distinct
        .cache()
      var neighbors: DataFrame = null
      // Expand the component, tracking the edges connecting to outside vertices in `numNeighbors`.
      // If there are too many, then add to component and iterate.
      var numNeighbors: Long = Long.MaxValue
      var subIter: Int = 0
      var lastCachedNeighbors: DataFrame = null
      var lastCachedComponent: DataFrame = component
      while (numNeighbors > HIGH_DEGREE) {
        println(s"handleSkew: SUB-ITERATION $subIter")
        // Add neighbors to component (except on the first iteration).
        // They are already disjoint, so no need to call distinct.
        if (neighbors != null) {
          component = component.unionAll(neighbors).cache()
        }
        // Get the neighbors of the component.
        neighbors = e.filter((e(SRC) !== hdv) && (e(DST) !== hdv)) // ignore edges to hdv
          .join(broadcast(component), e(SRC) === component(ID) || e(DST) === component(ID)) // 2nd-deg neighbor
          .select(when(e(SRC) === component(ID), e(DST)).otherwise(e(SRC)).as(ID)) // select 2nd-deg neighbor
          .except(component) // remove vertices already in component
          .distinct
          .cache()
        numNeighbors = neighbors.count()
        println(s"  numNeighbors = $numNeighbors")
        if (lastCachedNeighbors != null) lastCachedNeighbors.unpersist(blocking = false)
        lastCachedNeighbors = neighbors
        if (lastCachedComponent != null) lastCachedComponent.unpersist(blocking = false)
        lastCachedComponent = component
        subIter += 1
      }
      // Remove component from graph, except for hdv.
      bigComponents(hdv) = component
      e = e.filter((e(SRC) !== hdv) && (e(DST) !== hdv)) // remove edges to hdv
        .join(broadcast(component), e(SRC) === component(ID) || e(DST) === component(ID), "left_outer") // join with component vertices
        .where(col(ID).isNull) // remove edges to component vertices
        .select(e(SRC), e(DST))
      // Connect all neighbors with hdv.
      e = e.unionAll(neighbors.select(col(ID).as(SRC), lit(hdv).as(DST)))
      numVertices -= component.count()
      println(s" updated numVertices: $numVertices")
      e = checkpointDataFrame(e)
      // Get next highest-degree vertex.
      highestDegreeVertex = getHighestDegreeVertex(e, HIGH_DEGREE)
      // Handle caching.  Do not unpersist component since it is returned in bigComponents.
      if (lastCachedNeighbors != null) lastCachedNeighbors.unpersist(blocking = false)
      if (lastCachedE != null) lastCachedE.unpersist(blocking = false)
      lastCachedE = e
      iter += 1
    }
    (e, bigComponents.toMap)
  }

  def runOld(graph: GraphFrame): DataFrame = {
    // Create init column so that we can use addComponentColumn
    val INIT_COMPONENT_ID = "init_component"
    val v0 = addComponentColumn(graph.vertices, INIT_COMPONENT_ID)
    val initialValue: Column = col(INIT_COMPONENT_ID)
    val msgToDst = when(AggregateMessages.dst(COMPONENT_ID) > AggregateMessages.src(COMPONENT_ID),
      AggregateMessages.src(COMPONENT_ID))
    val msgToSrc = when(AggregateMessages.src(COMPONENT_ID) > AggregateMessages.dst(COMPONENT_ID),
      AggregateMessages.dst(COMPONENT_ID))
    val agg = min(AggregateMessages.msg)
    val vprog: Column = coalesce(col(COMPONENT_ID), AggregateMessages.msg)
    val maxIterations = 1000
    val g0 = GraphFrame(v0, graph.edges)

    val g = pregel(g0, initialValue, vprog, COMPONENT_ID, msgToSrc, msgToDst, agg, maxIterations)
    g.vertices
  }

  private def pregel(
      graph: GraphFrame,
      initialValue: Column,
      vprog: Column,
      resultName: String,
      msgToSrc: Column,
      msgToDst: Column,
      agg: Column,
      maxIterations: Int): GraphFrame = {

    val MSG = "MSG"  // TODO: Expose this from aggregateMessages

    // Initialize messages (MSG) with null
    var messages: DataFrame = graph.vertices.select(col(ID), lit(null).as(MSG))

    // Add initial result to vertices: resultName
    var v: DataFrame = graph.vertices.withColumn(resultName, initialValue)
    var iter = 0
    var activeMessageCount = Long.MaxValue
    while (iter < maxIterations && activeMessageCount > 0) {
      val prevVertices = v
      // Join messages with v to update vertices.
      val df = v.join(messages, ID)
      // vprog: (MSG, resultName) -> resultName
      v = df.withColumn(resultName, vprog).drop(df(resultName)).drop(MSG)
      v.cache()

      // Send new messages, and aggregate: (resultName -> MSG)
      val oldMessages = messages
      val g = GraphFrame(v, graph.edges)
      messages = g.aggregateMessages.sendToDst(msgToDst).sendToSrc(msgToSrc).agg(agg.as(MSG))
      messages.cache()
      // Check to see if we are done.
      //   NOTE: We need the new aggregateMessages which handles null messages for this check.
      // This materializes messages and prevVertices.
      activeMessageCount = messages.count()
      // Handle caching.
      oldMessages.unpersist(blocking = false)
      prevVertices.unpersist(blocking = false)
      iter += 1
    }
    messages.unpersist(blocking = false)
    GraphFrame(v, graph.edges)
  }

  private val COMPONENT_ID = "component"

  private def hasIntegralIdType(vertices: DataFrame): Boolean = {
    vertices.schema(ID).dataType match {
      case _ @ (ByteType | IntegerType | LongType | ShortType) => true
      case _ => false
    }
  }

  private def addComponentColumn(vertices: DataFrame, columnName: String): DataFrame = {
    if (hasIntegralIdType(vertices)) {
      vertices.withColumn(columnName, col(ID))
    } else {
      val indexedVertices = zipWithUniqueId(vertices)
      indexedVertices.withColumnRenamed("uniq_id", columnName).select("row.*", columnName)
    }
  }
}
