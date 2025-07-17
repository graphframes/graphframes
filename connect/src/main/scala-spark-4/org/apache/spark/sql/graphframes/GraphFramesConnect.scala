package org.apache.spark.sql.graphframes

import com.google.protobuf
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.plugin.RelationPlugin
import org.graphframes.connect.proto.GraphFramesAPI

import java.util.Optional

class GraphFramesConnect extends RelationPlugin {
  override def transform(
      relation: Array[Byte],
      planner: SparkConnectPlanner): Optional[LogicalPlan] = {
    val rel = protobuf.Any.parseFrom(relation)
    if (rel.is(classOf[GraphFramesAPI])) {
      val protoCall = rel.unpack(classOf[GraphFramesAPI])
      // Because the plugins API is changed in spark 4.0 it makes sense to separate plugin impl from the parsing logic
      val result = GraphFramesConnectUtils.parseAPICall(protoCall, planner)
      Optional.of(SparkShims.planFromDataFrame(result))
    } else {
      Optional.empty()
    }
  }
}
