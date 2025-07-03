package org.apache.spark.sql.graphframes

import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.storage.StorageLevel

object GraphFramesConf {
  private val USE_LABELS_AS_COMPONENTS =
    SQLConf
      .buildConf("spark.graphframes.useLabelsAsComponents")
      .doc(""" Tells the connected components algorithm to use (default: "true") labels as components in the output
          | DataFrame. If set to "false", randomly generated labels with the data type LONG will returned.
          |""".stripMargin)
      .version("0.9.0")
      .booleanConf
      .createWithDefault(true)

  private val CONNECTED_COMPONENTS_ALGORITHM =
    SQLConf
      .buildConf("spark.graphframes.connectedComponents.algorithm")
      .doc(""" Sets the connected components algorithm to use (default: "graphframes"). Supported algorithms
          |   - "graphframes": Uses alternating large star and small star iterations proposed in
          |     [[http://dx.doi.org/10.1145/2670979.2670997 Connected Components in MapReduce and Beyond]]
          |     with skewed join optimization.
          |   - "graphx": Converts the graph to a GraphX graph and then uses the connected components
          |     implementation in GraphX.
          | @see org.graphframes.lib.ConnectedComponents.supportedAlgorithms""".stripMargin)
      .version("0.9.0")
      .stringConf
      .createOptional

  private val CONNECTED_COMPONENTS_BROADCAST_THRESHOLD =
    SQLConf
      .buildConf("spark.graphframes.connectedComponents.broadcastthreshold")
      .doc(""" Sets broadcast threshold in propagating component assignments (default: 1000000). If a node
          | degree is greater than this threshold at some iteration, its component assignment will be
          | collected and then broadcasted back to propagate the assignment to its neighbors. Otherwise,
          | the assignment propagation is done by a normal Spark join. This parameter is only used when
          | the algorithm is set to "graphframes".""".stripMargin)
      .version("0.9.0")
      .intConf
      .createOptional

  private val CONNECTED_COMPONENTS_CHECKPOINT_INTERVAL =
    SQLConf
      .buildConf("spark.graphframes.connectedComponents.checkpointinterval")
      .doc(""" Sets checkpoint interval in terms of number of iterations (default: 2). Checkpointing
          | regularly helps recover from failures, clean shuffle files, shorten the lineage of the
          | computation graph, and reduce the complexity of plan optimization. As of Spark 2.0, the
          | complexity of plan optimization would grow exponentially without checkpointing. Hence,
          | disabling or setting longer-than-default checkpoint intervals are not recommended. Checkpoint
          | data is saved under `org.apache.spark.SparkContext.getCheckpointDir` with prefix
          | "connected-components". If the checkpoint directory is not set, this throws a
          | `java.io.IOException`. Set a nonpositive value to disable checkpointing. This parameter is
          | only used when the algorithm is set to "graphframes". Its default value might change in the
          | future.
          | @see `org.apache.spark.SparkContext.setCheckpointDir` in Spark API doc""".stripMargin)
      .version("0.9.0")
      .intConf
      .createOptional

  private val CONNECTED_COMPONENTS_INTERMEDIATE_STORAGE_LEVEL =
    SQLConf
      .buildConf("spark.graphframes.connectedComponents.intermediatestoragelevel")
      .doc("Sets storage level for intermediate datasets that require multiple passes (default: ``MEMORY_AND_DISK``).")
      .version("0.9.0")
      .stringConf
      .createOptional

  private def get(entry: ConfigEntry[_]): Option[String] = {
    try {
      Option(SparkSession.getActiveSession.get.conf.get(entry.key))
    } catch {
      case _: NoSuchElementException => None
    }
  }

  def getConnectedComponentsAlgorithm: Option[String] = {
    get(CONNECTED_COMPONENTS_ALGORITHM) match {
      case Some(threshold) => Some(threshold.toLowerCase)
      case _ => None
    }
  }

  def getConnectedComponentsBroadcastThreshold: Option[Int] = {
    get(CONNECTED_COMPONENTS_BROADCAST_THRESHOLD) match {
      case Some(threshold) => Some(threshold.toInt)
      case _ => None
    }
  }

  def getConnectedComponentsCheckpointInterval: Option[Int] = {
    get(CONNECTED_COMPONENTS_CHECKPOINT_INTERVAL) match {
      case Some(interval) => Some(interval.toInt)
      case _ => None
    }
  }

  def getConnectedComponentsStorageLevel: Option[StorageLevel] = {
    get(CONNECTED_COMPONENTS_INTERMEDIATE_STORAGE_LEVEL) match {
      case Some(level) => Some(StorageLevel.fromString(level.toUpperCase))
      case _ => None
    }
  }

  def getUseLabelsAsComponents: Boolean = get(USE_LABELS_AS_COMPONENTS).get.toBoolean
}
