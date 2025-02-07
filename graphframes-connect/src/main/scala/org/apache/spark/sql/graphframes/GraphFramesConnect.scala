package org.apache.spark.sql.graphframes

import org.graphframes.connect.proto.GraphFramesAPI

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.plugin.RelationPlugin

import com.google.protobuf

class GraphFramesConnect extends RelationPlugin {
  override def transform(
      relation: protobuf.Any,
      planner: SparkConnectPlanner): Option[LogicalPlan] = {
    if (relation.is(classOf[GraphFramesAPI])) {
      val protoCall = relation.unpack(classOf[GraphFramesAPI])
      // Because the plugins API is changed in spark 4.0 it makes sense to separate plugin impl from the parsing logic
      Option(GraphFramesConnectUtils.parseAPICall(protoCall, planner).logicalPlan)
    } else {
      Option.empty
    }
  }
}
