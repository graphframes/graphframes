// Because Dataset.ofRows is private[sql] we are forced to use spark package
// Same about Column helper object.
package org.apache.spark.sql.graphframes

import scala.jdk.CollectionConverters._
import org.graphframes.{GraphFrame, GraphFramesUnreachableException}
import org.graphframes.connect.proto.{ColumnOrExpression, GraphFramesAPI, StringOrLongID}
import org.graphframes.connect.proto.ColumnOrExpression.ColOrExprCase
import org.graphframes.connect.proto.GraphFramesAPI.MethodCase
import org.graphframes.connect.proto.StringOrLongID.IdCase
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.functions.{col, expr, lit}
import com.google.protobuf.ByteString

object GraphFramesConnectUtils {
  private[graphframes] def parseColumnOrExpression(
      colOrExpr: ColumnOrExpression,
      planner: SparkConnectPlanner): Column = {
    colOrExpr.getColOrExprCase match {
      case ColOrExprCase.COL =>
        SparkShims.createColumn(
          planner.transformExpression(
            org.apache.spark.connect.proto.Expression.parseFrom(colOrExpr.getCol.toByteArray)))
      case ColOrExprCase.EXPR => expr(colOrExpr.getExpr)
      case _ =>
        throw new RuntimeException(
          "INTERNAL ERROR: unreachable case in function parseColumnOrExpression")
    }
  }

  private[graphframes] def parseLongOrStringID(id: StringOrLongID): Any = {
    id.getIdCase match {
      case IdCase.LONG_ID => id.getLongId
      case IdCase.STRING_ID => id.getStringId
      case _ =>
        throw new RuntimeException(
          "INTERNAL ERROR: unreachable case in function parseLongOrStringID")
    }
  }

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

  private[graphframes] def extractGraphFrame(
      apiMessage: GraphFramesAPI,
      planner: SparkConnectPlanner): GraphFrame = {
    val vertices = parseDataFrame(apiMessage.getVertices, planner)
    val edges = parseDataFrame(apiMessage.getEdges, planner)

    GraphFrame(vertices, edges)
  }

  private[graphframes] def parseAPICall(
      apiMessage: GraphFramesAPI,
      planner: SparkConnectPlanner): DataFrame = {
    val graphFrame = extractGraphFrame(apiMessage, planner)

    apiMessage.getMethodCase match {
      case MethodCase.AGGREGATE_MESSAGES => {
        val aggregateMessagesProto = apiMessage.getAggregateMessages
        var aggregateMessages = graphFrame.aggregateMessages
        if (aggregateMessagesProto.hasSendToDst) {
          aggregateMessages = aggregateMessages.sendToDst(
            parseColumnOrExpression(aggregateMessagesProto.getSendToDst, planner))
        }
        if (aggregateMessagesProto.hasSendToSrc) {
          aggregateMessages = aggregateMessages.sendToSrc(
            parseColumnOrExpression(aggregateMessagesProto.getSendToSrc, planner))
        }

        aggregateMessages.agg(parseColumnOrExpression(aggregateMessagesProto.getAggCol, planner))
      }
      case MethodCase.BFS => {
        val bfsProto = apiMessage.getBfs
        graphFrame.bfs
          .toExpr(parseColumnOrExpression(bfsProto.getToExpr, planner))
          .fromExpr(parseColumnOrExpression(bfsProto.getFromExpr, planner))
          .edgeFilter(parseColumnOrExpression(bfsProto.getEdgeFilter, planner))
          .maxPathLength(bfsProto.getMaxPathLength)
          .run()
      }
      case MethodCase.CONNECTED_COMPONENTS => {
        val cc = apiMessage.getConnectedComponents
        graphFrame.connectedComponents
          .setAlgorithm(cc.getAlgorithm)
          .setCheckpointInterval(cc.getCheckpointInterval)
          .setBroadcastThreshold(cc.getBroadcastThreshold)
          .run()
      }
      case MethodCase.DROP_ISOLATED_VERTICES => {
        graphFrame.dropIsolatedVertices().vertices
      }
      case MethodCase.FILTER_EDGES => {
        val condition = parseColumnOrExpression(apiMessage.getFilterEdges.getCondition, planner)
        graphFrame.filterEdges(condition).edges
      }
      case MethodCase.FILTER_VERTICES => {
        val condition =
          parseColumnOrExpression(apiMessage.getFilterVertices.getCondition, planner)
        graphFrame.filterVertices(condition).vertices
      }
      case MethodCase.FIND => {
        graphFrame.find(apiMessage.getFind.getPattern)
      }
      case MethodCase.LABEL_PROPAGATION => {
        graphFrame.labelPropagation.maxIter(apiMessage.getLabelPropagation.getMaxIter).run()
      }
      case MethodCase.PAGE_RANK => {
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
      case MethodCase.PARALLEL_PERSONALIZED_PAGE_RANK => {
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
      case MethodCase.POWER_ITERATION_CLUSTERING => {
        val pic = apiMessage.getPowerIterationClustering
        if (pic.hasWeightCol) {
          graphFrame.powerIterationClustering(pic.getK, pic.getMaxIter, Some(pic.getWeightCol))
        } else {
          graphFrame.powerIterationClustering(pic.getK, pic.getMaxIter, None)
        }
      }
      case MethodCase.PREGEL => {
        val pregelProto = apiMessage.getPregel
        var pregel = graphFrame.pregel
          .aggMsgs(parseColumnOrExpression(pregelProto.getAggMsgs, planner))
          .setCheckpointInterval(pregelProto.getCheckpointInterval)
          .withVertexColumn(
            pregelProto.getAdditionalColName,
            parseColumnOrExpression(pregelProto.getAdditionalColInitial, planner),
            parseColumnOrExpression(pregelProto.getAdditionalColUpd, planner))
          .setMaxIter(pregelProto.getMaxIter)

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
      case MethodCase.SHORTEST_PATHS => {
        graphFrame.shortestPaths
          .landmarks(
            apiMessage.getShortestPaths.getLandmarksList.asScala.map(parseLongOrStringID).toSeq)
          .run()
      }
      case MethodCase.STRONGLY_CONNECTED_COMPONENTS => {
        graphFrame.stronglyConnectedComponents
          .maxIter(apiMessage.getStronglyConnectedComponents.getMaxIter)
          .run()
      }
      case MethodCase.SVD_PLUS_PLUS => {
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
      case MethodCase.TRIANGLE_COUNT => {
        graphFrame.triangleCount.run()
      }
      case MethodCase.TRIPLETS => {
        graphFrame.triplets
      }
      case _ => throw new GraphFramesUnreachableException() // Unreachable
    }
  }
}
