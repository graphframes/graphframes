package org.apache.spark.sql.graphframes

import com.google.protobuf.Any
import com.google.protobuf.InvalidProtocolBufferException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.plugin.RelationPlugin
import org.graphframes.connect.proto.GraphFramesAPI
import java.util.Optional
import org.apache.spark.sql.classic.{DataFrame => ClassicDataFrame}

class GraphFramesConnect extends RelationPlugin {
  override def transform(
      relation: Array[Byte],
      planner: SparkConnectPlanner): Optional[LogicalPlan] = {
    try {
      val relationProto = Any.parseFrom(relation)
      if (relationProto.is(classOf[GraphFramesAPI])) {
        val protoCall = relationProto.unpack(classOf[GraphFramesAPI])
        val result = GraphFramesConnectUtils.parseAPICall(protoCall, planner)
        // We know exactly that it is classic one!
        Optional.of(result.asInstanceOf[ClassicDataFrame].logicalPlan)
      } else {
        Optional.empty()
      }
    } catch {
      case e: InvalidProtocolBufferException => throw new RuntimeException(e)
    }
  }
}
