// Because Dataset.ofRows is private[sql], we are forced to use spark package;
// Same about a Column helper object.
package org.apache.spark.sql.graphframes

import com.google.protobuf.ByteString
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.lit
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame
import org.graphframes.GraphFramesUnreachableException
import org.graphframes.connect.proto

import scala.jdk.CollectionConverters.*

/**
 * Utility object providing helper methods for parsing and transforming data structures related to
 * GraphFrames and enabling interaction between GraphFrames and Spark Connect APIs.
 *
 * The methods in this object are intended for internal use within the GraphFrames module
 * (`private[graphframes]`) to support parsing, transformation, and execution of GraphFrame API
 * calls based on serialized or protocol buffer inputs.
 */
object GraphFramesConnectUtils {

  /**
   * Parses a protobuf StorageLevel object and converts it to a corresponding Spark StorageLevel.
   *
   * @param pbStorageLevel
   *   the protobuf StorageLevel object to be parsed
   * @return
   *   the corresponding Spark StorageLevel
   */
  private[graphframes] def parseStorageLevel(pbStorageLevel: proto.StorageLevel): StorageLevel = {
    pbStorageLevel.getStorageLevelCase match {
      case proto.StorageLevel.StorageLevelCase.DISK_ONLY => StorageLevel.DISK_ONLY
      case proto.StorageLevel.StorageLevelCase.DISK_ONLY_2 => StorageLevel.DISK_ONLY_2
      case proto.StorageLevel.StorageLevelCase.DISK_ONLY_3 => StorageLevel.DISK_ONLY_3
      case proto.StorageLevel.StorageLevelCase.MEMORY_AND_DISK => StorageLevel.MEMORY_AND_DISK_SER
      case proto.StorageLevel.StorageLevelCase.MEMORY_AND_DISK_2 =>
        StorageLevel.MEMORY_AND_DISK_SER_2
      case proto.StorageLevel.StorageLevelCase.MEMORY_AND_DISK_DESER =>
        StorageLevel.MEMORY_AND_DISK
      case proto.StorageLevel.StorageLevelCase.MEMORY_ONLY => StorageLevel.MEMORY_ONLY_SER
      case proto.StorageLevel.StorageLevelCase.MEMORY_ONLY_2 => StorageLevel.MEMORY_ONLY_SER_2
      case _ => throw new GraphFramesUnreachableException()
    }
  }

  /**
   * Parses a proto.ColumnOrExpression object and converts it to a corresponding Spark Column.
   *
   * @param colOrExpr
   *   the proto.ColumnOrExpression object to be parsed
   * @param planner
   *   the SparkConnectPlanner used for transforming expressions
   * @return
   *   the resulting Spark Column
   */
  private[graphframes] def parseColumnOrExpression(
      colOrExpr: proto.ColumnOrExpression,
      planner: SparkConnectPlanner): Column = {
    colOrExpr.getColOrExprCase match {
      case proto.ColumnOrExpression.ColOrExprCase.COL =>
        SparkShims.createColumn(
          planner.transformExpression(
            org.apache.spark.connect.proto.Expression.parseFrom(colOrExpr.getCol.toByteArray)))
      case proto.ColumnOrExpression.ColOrExprCase.EXPR => expr(colOrExpr.getExpr)
      case _ =>
        throw new GraphFramesUnreachableException()
    }
  }

  /**
   * Converts a proto.StringOrLongID object to its corresponding Scala representation.
   *
   * @param id
   *   the proto.StringOrLongID object to be parsed
   * @return
   *   the Scala representation of the ID (String or Long)
   * @throws GraphFramesUnreachableException
   *   if the ID case is unrecognized
   */
  private[graphframes] def parseLongOrStringID(id: proto.StringOrLongID): Any = {
    id.getIdCase match {
      case proto.StringOrLongID.IdCase.LONG_ID => id.getLongId
      case proto.StringOrLongID.IdCase.STRING_ID => id.getStringId
      case _ =>
        throw new GraphFramesUnreachableException()
    }
  }

  /**
   * Parses the given serialized data to construct a Spark DataFrame.
   *
   * @param data
   *   the serialized representation of the DataFrame in ByteString format. Must not be empty.
   * @param planner
   *   the SparkConnectPlanner instance used to transform the serialized plan into a Spark
   *   DataFrame.
   * @return
   *   the resulting Spark DataFrame created from the provided data.
   * @throws IllegalArgumentException
   *   if the given data is empty.
   */
  private[graphframes] def parseDataFrame(
      data: ByteString,
      planner: SparkConnectPlanner): DataFrame = {
    if (data.isEmpty) {
      throw new IllegalArgumentException(
        "Expected a serialized DataFrame but got an empty ByteString.")
    }
    SparkShims.createDataFrame(
      planner.sessionHolder.session,
      planner.transformRelation(
        org.apache.spark.connect.proto.Plan.parseFrom(data.toByteArray).getRoot))
  }

  /**
   * Extracts a GraphFrame from the provided GraphFramesAPI message using the specified planner.
   *
   * @param apiMessage
   *   the GraphFramesAPI protobuf message containing serialized vertices and edges
   * @param planner
   *   the SparkConnectPlanner used for parsing and constructing DataFrames
   * @return
   *   the constructed GraphFrame consisting of vertices and edges
   */
  private[graphframes] def extractGraphFrame(
      apiMessage: proto.GraphFramesAPI,
      planner: SparkConnectPlanner): GraphFrame = {
    val vertices = parseDataFrame(apiMessage.getVertices, planner)
    val edges = parseDataFrame(apiMessage.getEdges, planner)

    GraphFrame(vertices, edges)
  }

  /**
   * Parses a GraphFrames API call from a protocol buffer message and executes the corresponding
   * operation on the GraphFrame object obtained from the planner.
   *
   * @param apiMessage
   *   The protocol buffer message that defines the GraphFrames API operation and its parameters.
   * @param planner
   *   A SparkConnectPlanner instance used to translate protocol buffer expressions into Spark SQL
   *   objects (e.g., DataFrame, Column).
   * @return
   *   A DataFrame that represents the result of the executed GraphFrame operation.
   */
  private[graphframes] def parseAPICall(
      apiMessage: proto.GraphFramesAPI,
      planner: SparkConnectPlanner): DataFrame = {
    val graphFrame = extractGraphFrame(apiMessage, planner)

    apiMessage.getMethodCase match {
      case proto.GraphFramesAPI.MethodCase.AGGREGATE_MESSAGES => {
        val aggregateMessagesProto = apiMessage.getAggregateMessages
        var aggregateMessages = graphFrame.aggregateMessages
        if (aggregateMessagesProto.getSendToDstList.size() == 1) {
          aggregateMessages = aggregateMessages.sendToDst(
            parseColumnOrExpression(aggregateMessagesProto.getSendToDst(0), planner))
        } else if (aggregateMessagesProto.getSendToDstList.size() > 1) {
          val sendToDst = aggregateMessagesProto.getSendToDstList.asScala.map(
            parseColumnOrExpression(_, planner))
          aggregateMessages =
            aggregateMessages.sendToDst(sendToDst.head, sendToDst.tail.toSeq: _*)
        }
        if (aggregateMessagesProto.getSendToSrcList.size() == 1) {
          aggregateMessages = aggregateMessages.sendToSrc(
            parseColumnOrExpression(aggregateMessagesProto.getSendToSrc(0), planner))
        } else if (aggregateMessagesProto.getSendToSrcList.size() > 1) {
          val sendToSrc = aggregateMessagesProto.getSendToSrcList.asScala.map(
            parseColumnOrExpression(_, planner))
          aggregateMessages =
            aggregateMessages.sendToSrc(sendToSrc.head, sendToSrc.tail.toSeq: _*)
        }

        if (aggregateMessagesProto.hasStorageLevel) {
          aggregateMessages = aggregateMessages.setIntermediateStorageLevel(
            parseStorageLevel(aggregateMessagesProto.getStorageLevel))
        }

        val aggCols =
          aggregateMessagesProto.getAggColList.asScala.map(parseColumnOrExpression(_, planner))

        // At least one agg col is required, and it is easier to check it on the client side
        if (aggCols.size == 1) {
          aggregateMessages.agg(aggCols.head)
        } else {
          aggregateMessages.agg(aggCols.head, aggCols.tail.toSeq: _*)
        }
      }
      case proto.GraphFramesAPI.MethodCase.BFS => {
        val bfsProto = apiMessage.getBfs
        graphFrame.bfs
          .toExpr(parseColumnOrExpression(bfsProto.getToExpr, planner))
          .fromExpr(parseColumnOrExpression(bfsProto.getFromExpr, planner))
          .edgeFilter(parseColumnOrExpression(bfsProto.getEdgeFilter, planner))
          .maxPathLength(bfsProto.getMaxPathLength)
          .run()
      }
      case proto.GraphFramesAPI.MethodCase.CONNECTED_COMPONENTS => {
        val cc = apiMessage.getConnectedComponents
        val ccBuilder = graphFrame.connectedComponents
          .maxIter(cc.getMaxIter)
          .setAlgorithm(cc.getAlgorithm)
          .setCheckpointInterval(cc.getCheckpointInterval)
          .setBroadcastThreshold(cc.getBroadcastThreshold)
          .setUseLocalCheckpoints(cc.getUseLocalCheckpoints)
          .setUseLabelsAsComponents(cc.getUseLabelsAsComponents)

        if (cc.hasStorageLevel) {
          ccBuilder.setIntermediateStorageLevel(parseStorageLevel(cc.getStorageLevel)).run()
        } else {
          ccBuilder.run()
        }
      }

      case proto.GraphFramesAPI.MethodCase.DETECTING_CYCLES => {
        val dc = apiMessage.getDetectingCycles
        val dcBuilder = graphFrame.detectingCycles
          .setCheckpointInterval(dc.getCheckpointInterval)
          .setUseLocalCheckpoints(dc.getUseLocalCheckpoints)
        if (dc.hasStorageLevel) {
          dcBuilder.setIntermediateStorageLevel(parseStorageLevel(dc.getStorageLevel)).run()
        } else {
          dcBuilder.run()
        }
      }

      case proto.GraphFramesAPI.MethodCase.DROP_ISOLATED_VERTICES => {
        graphFrame.dropIsolatedVertices().vertices
      }
      case proto.GraphFramesAPI.MethodCase.FILTER_EDGES => {
        val condition = parseColumnOrExpression(apiMessage.getFilterEdges.getCondition, planner)
        graphFrame.filterEdges(condition).edges
      }
      case proto.GraphFramesAPI.MethodCase.FILTER_VERTICES => {
        val condition =
          parseColumnOrExpression(apiMessage.getFilterVertices.getCondition, planner)
        graphFrame.filterVertices(condition).vertices
      }
      case proto.GraphFramesAPI.MethodCase.FIND => {
        graphFrame.find(apiMessage.getFind.getPattern)
      }
      case proto.GraphFramesAPI.MethodCase.LABEL_PROPAGATION => {
        val lp = apiMessage.getLabelPropagation
        val lpBuilder = graphFrame.labelPropagation
          .maxIter(lp.getMaxIter)
          .setAlgorithm(lp.getAlgorithm)
          .setCheckpointInterval(lp.getCheckpointInterval)
          .setUseLocalCheckpoints(lp.getUseLocalCheckpoints)

        if (lp.hasStorageLevel) {
          lpBuilder.setIntermediateStorageLevel(parseStorageLevel(lp.getStorageLevel)).run()
        } else {
          lpBuilder.run()
        }
      }
      case proto.GraphFramesAPI.MethodCase.PAGE_RANK => {
        val pageRankProto = apiMessage.getPageRank
        val pageRank = graphFrame.pageRank.resetProbability(pageRankProto.getResetProbability)

        if (pageRankProto.hasMaxIter) {
          pageRank.maxIter(pageRankProto.getMaxIter)
        } else {
          pageRank.tol(pageRankProto.getTol)
        }

        if (pageRankProto.hasSourceId) {
          pageRank.sourceId(parseLongOrStringID(pageRankProto.getSourceId))
        }

        // Edges should be updated on the client side
        // TODO: do we really need an edge weights in that case?
        // see comments in the Python API
        pageRank.run().vertices
      }
      case proto.GraphFramesAPI.MethodCase.PARALLEL_PERSONALIZED_PAGE_RANK => {
        val pPageRankProto = apiMessage.getParallelPersonalizedPageRank
        val sourceIds = pPageRankProto.getSourceIdsList.asScala
          .map(parseLongOrStringID)
          .toArray
        val pPageRank = graphFrame.parallelPersonalizedPageRank
        pPageRank
          .resetProbability(pPageRankProto.getResetProbability)
          .maxIter(pPageRankProto.getMaxIter)
          .sourceIds(sourceIds)
          .run()
          .vertices // See comment in the PageRank
      }
      case proto.GraphFramesAPI.MethodCase.POWER_ITERATION_CLUSTERING => {
        val pic = apiMessage.getPowerIterationClustering
        if (pic.hasWeightCol) {
          graphFrame.powerIterationClustering(pic.getK, pic.getMaxIter, Some(pic.getWeightCol))
        } else {
          graphFrame.powerIterationClustering(pic.getK, pic.getMaxIter, None)
        }
      }
      case proto.GraphFramesAPI.MethodCase.PREGEL => {
        val pregelProto = apiMessage.getPregel
        var pregel = graphFrame.pregel
          .aggMsgs(parseColumnOrExpression(pregelProto.getAggMsgs, planner))
          .setCheckpointInterval(pregelProto.getCheckpointInterval)
          .withVertexColumn(
            pregelProto.getAdditionalColName,
            parseColumnOrExpression(pregelProto.getAdditionalColInitial, planner),
            parseColumnOrExpression(pregelProto.getAdditionalColUpd, planner))
          .setMaxIter(pregelProto.getMaxIter)
          .setUseLocalCheckpoints(pregelProto.getUseLocalCheckpoints)

        if (pregelProto.hasStorageLevel) {
          pregel =
            pregel.setIntermediateStorageLevel(parseStorageLevel(pregelProto.getStorageLevel))
        }

        if (pregelProto.hasInitialActiveExpr) {
          // We are not checking here that all the attrs are present;
          // Check should be done on the client side.
          pregel = pregel
            .setInitialActiveVertexExpression(
              parseColumnOrExpression(pregelProto.getInitialActiveExpr, planner))
            .setUpdateActiveVertexExpression(
              parseColumnOrExpression(pregelProto.getUpdateActiveExpr, planner))

          if (pregelProto.hasSkipMessagesFromNonActive) {
            pregel = pregel.setSkipMessagesFromNonActiveVertices(
              pregelProto.getSkipMessagesFromNonActive)
          }

          if (pregelProto.hasStopIfAllNonActive) {
            pregel = pregel.setStopIfAllNonActiveVertices(pregelProto.getStopIfAllNonActive)
          }
        }

        pregel = pregelProto.getSendMsgToSrcList.asScala
          .map(parseColumnOrExpression(_, planner))
          .foldLeft(pregel)((p, col) => p.sendMsgToSrc(col))
        pregel = pregelProto.getSendMsgToDstList.asScala
          .map(parseColumnOrExpression(_, planner))
          .foldLeft(pregel)((p, col) => p.sendMsgToDst(col))

        if (pregelProto.hasEarlyStopping) {
          pregel = pregel.setEarlyStopping(pregelProto.getEarlyStopping)
        }

        pregel.run()
      }
      case proto.GraphFramesAPI.MethodCase.SHORTEST_PATHS => {
        val spBuilder = graphFrame.shortestPaths
          .landmarks(
            apiMessage.getShortestPaths.getLandmarksList.asScala.map(parseLongOrStringID).toSeq)
          .setAlgorithm(apiMessage.getShortestPaths.getAlgorithm)
          .setCheckpointInterval(apiMessage.getShortestPaths.getCheckpointInterval)
          .setUseLocalCheckpoints(apiMessage.getShortestPaths.getUseLocalCheckpoints)

        if (apiMessage.getShortestPaths.hasStorageLevel) {
          spBuilder
            .setIntermediateStorageLevel(
              parseStorageLevel(apiMessage.getShortestPaths.getStorageLevel))
            .run()
        } else {
          spBuilder.run()
        }
      }
      case proto.GraphFramesAPI.MethodCase.STRONGLY_CONNECTED_COMPONENTS => {
        graphFrame.stronglyConnectedComponents
          .maxIter(apiMessage.getStronglyConnectedComponents.getMaxIter)
          .run()
      }
      case proto.GraphFramesAPI.MethodCase.SVD_PLUS_PLUS => {
        val svdPPProto = apiMessage.getSvdPlusPlus
        val svd = graphFrame.svdPlusPlus
          .maxIter(svdPPProto.getMaxIter)
          .gamma1(svdPPProto.getGamma1)
          .gamma2(svdPPProto.getGamma2)
          .gamma6(svdPPProto.getGamma6)
          .gamma7(svdPPProto.getGamma7)
          .rank(svdPPProto.getRank)
          .minValue(svdPPProto.getMinValue)
          .maxValue(svdPPProto.getMaxValue)
        val svdResult = svd.run()
        svdResult.withColumn("loss", lit(svd.loss))
      }
      case proto.GraphFramesAPI.MethodCase.TRIANGLE_COUNT => {
        val trCounter = graphFrame.triangleCount

        if (apiMessage.getTriangleCount.hasStorageLevel) {
          trCounter
            .setIntermediateStorageLevel(
              parseStorageLevel(apiMessage.getTriangleCount.getStorageLevel))
            .run()
        } else {
          trCounter.run()
        }
      }
      case proto.GraphFramesAPI.MethodCase.TRIPLETS => {
        graphFrame.triplets
      }
      case proto.GraphFramesAPI.MethodCase.KCORE => {
        var kCoreBuilder =
          graphFrame.kCore
            .setCheckpointInterval(apiMessage.getKcore.getCheckpointInterval)
            .setUseLocalCheckpoints(apiMessage.getKcore.getUseLocalCheckpoints)

        if (apiMessage.getKcore.hasStorageLevel) {
          kCoreBuilder = kCoreBuilder.setIntermediateStorageLevel(
            parseStorageLevel(apiMessage.getKcore.getStorageLevel))
        }

        kCoreBuilder.run()
      }
      case _ => throw new GraphFramesUnreachableException() // Unreachable
    }
  }
}
