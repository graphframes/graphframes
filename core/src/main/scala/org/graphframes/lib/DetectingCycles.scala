package org.graphframes.lib

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame
import org.graphframes.Logging
import org.graphframes.WithCheckpointInterval
import org.graphframes.WithIntermediateStorageLevel
import org.graphframes.WithLocalCheckpoints

class DetectingCycles private[graphframes] (private val graph: GraphFrame)
    extends Arguments
    with Serializable
    with Logging
    with WithIntermediateStorageLevel
    with WithLocalCheckpoints
    with WithCheckpointInterval {
  import DetectingCycles._
  def run(): DataFrame = {
    val rawRes = DetectingCycles.run(
      graph,
      useLocalCheckpoints,
      checkpointInterval,
      intermediateStorageLevel)
    val explodedRes = rawRes
      .select(
        col(GraphFrame.ID),
        filter(col(foundSeqCol), x => size(x) > lit(0)).alias(foundSeqCol))
      .filter(size(col(foundSeqCol)) > lit(0))
      .select(
        col(GraphFrame.ID),
        // from vid -> [[cycle1, cycle2, ...]]
        // to vid -> [cycle1], vid -> [cycle2], ...
        explode(col(foundSeqCol)).alias(foundSeqCol))
      .persist(intermediateStorageLevel)
    explodedRes.count()
    resultIsPersistent()
    rawRes.unpersist()
    explodedRes
  }
}

object DetectingCycles {
  private val storedSeqCol: String = "sequences"
  val foundSeqCol: String = "found_cycles"

  def run(
      graph: GraphFrame,
      useLocalCheckpoints: Boolean,
      checkpointInterval: Int,
      intermediateStorageLevel: StorageLevel): DataFrame = {
    val preparedGraph = GraphFrame(
      graph.vertices.select(GraphFrame.ID),
      graph.edges.select(GraphFrame.SRC, GraphFrame.DST))

    val vertexDT = preparedGraph.vertices.schema(GraphFrame.ID).dataType

    // Each vertex stores sequences from the previous iteration, initial is just Array(Array(ID))
    val initSequences = array(array(col(GraphFrame.ID)))
    // Each vertex stores all the found cycles
    val foundSequences = array().cast(ArrayType(ArrayType(vertexDT)))
    // Message is simply stored sequences
    val sentMessages = when(size(Pregel.src(storedSeqCol)) =!= lit(0), Pregel.src(storedSeqCol))
      .otherwise(lit(null).cast(ArrayType(ArrayType(vertexDT))))
    // If the sequence contains the current vertex ID somewhere in the middle, it is
    // a previously detected cycle and a sequence should be discarded.
    val filterOutSequences = flatten(collect_list(Pregel.msg))
    when(Pregel.msg.isNull, array(array()).cast(ArrayType(ArrayType(vertexDT))))
      .otherwise(filter(Pregel.msg, x => !(array_position(x, col(GraphFrame.ID)) > lit(1))))
    // update found sequences by appending all from messages that start from the current vertex ID
    val updateFound = when(Pregel.msg.isNull, col(foundSeqCol)).otherwise(
      array_union(
        col(foundSeqCol),
        transform(
          filter(Pregel.msg, x => try_element_at(x, lit(1)) === col(GraphFrame.ID)),
          x => array_append(x, col(GraphFrame.ID)))))
    // update stored sequences by filtering out already added sequences
    val updateSequences = transform(
      filter(Pregel.msg, x => !array_contains(x, col(GraphFrame.ID))),
      x => array_append(x, col(GraphFrame.ID)))

    preparedGraph.pregel
      .setCheckpointInterval(checkpointInterval)
      .setUseLocalCheckpoints(useLocalCheckpoints)
      .setIntermediateStorageLevel(intermediateStorageLevel)
      .setEarlyStopping(false)
      .setSkipMessagesFromNonActiveVertices(true)
      .setInitialActiveVertexExpression(lit(true))
      .sendMsgToDst(sentMessages)
      .setUpdateActiveVertexExpression(Pregel.msg.isNotNull && (size(updateSequences) > lit(0)))
      .withVertexColumn(storedSeqCol, initSequences, updateSequences)
      .withVertexColumn(foundSeqCol, foundSequences, updateFound)
      .aggMsgs(filterOutSequences)
      .run()
  }
}
