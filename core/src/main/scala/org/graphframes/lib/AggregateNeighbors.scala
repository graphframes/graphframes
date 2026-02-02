package org.graphframes.lib

import org.graphframes.GraphFrame
import org.graphframes.Logging
import org.graphframes.WithIntermediateStorageLevel
import org.graphframes.WithCheckpointInterval
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.functions.*
import org.graphframes.WithLocalCheckpoints

class AggregateNeighbors private[graphframes] (graph: GraphFrame)
    extends Serializable
    with Logging
    with WithIntermediateStorageLevel
    with WithCheckpointInterval
    with WithLocalCheckpoints {

  import AggregateNeighbors.*

  private var startingVertices: Column = lit(true)

  private var maxHops: Int = 3
  private var stoppingConditions: Seq[Column] = Seq.empty
  private var accumulatorsNames: Seq[String] = Seq.empty
  private var accumulatorsInits: Seq[Column] = Seq.empty
  private var accumulatorsUpdates: Seq[Column] = Seq.empty

  private var requiredVertexAttributes: Seq[String] = Seq.empty
  private var requiredEdgeAttributes: Seq[String] = Seq.empty

  def setStartingVertices(value: Column): this.type = {
    this.startingVertices = value
    this
  }

  def setMaxHops(value: Int): this.type = {
    this.maxHops = value
    this
  }

  def setStoppingConditions(values: Column*): this.type = {
    this.stoppingConditions = values
    this
  }

  def setAccumulators(names: Seq[String], inits: Seq[Column], updates: Seq[Column]): this.type = {
    require(
      inits.size == updates.size && updates.size == names.size,
      "Inits, updates and names must have the same size.")
    this.accumulatorsNames = names
    this.accumulatorsInits = inits
    this.accumulatorsUpdates = updates
    this
  }

  def addAccumulator(name: String, init: Column, update: Column): this.type = {
    this.accumulatorsNames :+= name
    this.accumulatorsInits :+= init
    this.accumulatorsUpdates :+= update
    this
  }

  def setRequiredVertexAttributes(values: Seq[String]): this.type = {
    this.requiredVertexAttributes = values
    this
  }

  def setRequiredEdgeAttributes(values: Seq[String]): this.type = {
    this.requiredEdgeAttributes = values
    this
  }

  def run(): DataFrame = {
    require(maxHops > 0, "maxHops must be greater than 0")
    if (maxHops > 10)
      logWarn(s"maxHops is very large ($maxHops). This might be performance-intensive.")
    require(accumulatorsNames.nonEmpty, "At least one accumulator must be added")

    val reqAttrs = (if (requiredVertexAttributes.isEmpty) {
                      graph.vertices.columns.toSeq
                    } else {
                      requiredVertexAttributes :+ GraphFrame.ID
                    }).map(col(_))

    val reqEdgeAttr = (if (requiredEdgeAttributes.isEmpty) {
                         graph.edges.columns.toSeq
                       } else {
                         requiredEdgeAttributes ++ Seq(GraphFrame.SRC, GraphFrame.DST)
                       }).map(col(_))

    val verticesWithAttributes = graph.vertices.select(
      col(GraphFrame.ID).alias("dst_id"),
      struct(reqAttrs: _*).alias(dstAttributes))

    // "right" side of the join for each iteration
    val semiTriplets = graph.edges
      .select(
        col(GraphFrame.SRC),
        col(GraphFrame.DST),
        struct(reqEdgeAttr: _*).alias(edgeAttributes))
      .join(verticesWithAttributes, col("dst_id") === col(GraphFrame.DST), "left")
      .persist(intermediateStorageLevel)

    // memory-tracking
    val persistanceQueue = collection.mutable.Queue.empty[DataFrame]

    // holder of the current state of accumulators
    var states = graph.vertices
      .filter(startingVertices)
      .withColumns(accumulatorsNames.zip(accumulatorsInits).toMap)
      .withColumnRenamed(GraphFrame.ID, "src_id")
      .withColum(currentPathLenColName, lit(0))
      .withColumn(stoppingCondColName, lit(false))
      .select(accumulatorsNames ++ Seq("src_id", currentPathLenColName, stoppingCondColName): _*)
      .persist(intermediateStorageLevel)

    // holder of the finished accumulators
    var finished = states
      .filter(col(stoppingCondColName))
      .select(accumulatorsNames ++ Seq("src_id", currentPathLenColName))
      .withColumnRanamed("src_id", GraphFrame.ID)
      .persist(intermediateStorageLevel)

    var collected = finished.count()

    persistanceQueue += states
    persistanceQueue += finished

    var converged = states.isEmpty()
    var iter = 0

    while ((!converged) && (iter < maxHops)) {
      iter += 1
      // get full triplets
      val fullTriplets = semiTriplets.join(states, col(GraphFrame.SRC) === col("src_id"), "left")
      var colsToSelect = accumulatorsUpdates
        .zip(accumulatorsNames)
        .map(r => r._1.alias(r._2))
        .toSeq

      colsToSelect =
        colsToSelect :+ stoppingConditions.reduce((a, b) => a || b).alias(stoppingCondColName)
      colsToSelect = colsToSelect :+ lit(iter).alias(currentPathLenColName) :+ col("src_id")
      val updatedStates = fullTriplets.select(colsToSelect: _*)

      var newStates = updatedStates.filter(!col(stoppingCondColName))
      var newFinished = finished.unionByName(
        updatedStates.filter(
          col(stoppingCondColName)
            .select(accumulatorsNames ++ Seq("src_id", currentPathLenColName))
            .withColumnRenamed("src_id", GraphFrame.ID)))

      if ((checkpointInterval > 0) && (iter % checkpointInterval == 0)) {
        if (useLocalCheckpoints) {
          newStates = newStates.localCheckpoint()
          newFinished = newFinished.localCheckpoint()
        } else {
          newStates = newStates.checkpoint()
          newFinished = newFinished.checkpoint()
        }
      } else {
        newStates = newStates.persist(intermediateStorageLevel)
        newFinished = newFinished.persist(intermediateStorageLevel)

        
      }
    }

    null
  }
}

object AggregateNeighbors extends Serializable {
  private val stoppingCondColName: String = "_stopped"
  private val currentPathLenColName: String = "hop"
  private val srcAttributes: String = "src_attributes"
  private val dstAttributes: String = "dst_attributes"
  private val edgeAttributes: String = "edge_attributes"

  def srcAttr(name: String): Column = col(srcAttributes).getField(name)

  def dstAttr(name: String): Column = col(dstAttributes).getField(name)

  def edgeAttr(name: String): Column = col(edgeAttributes).getField(name)
}
