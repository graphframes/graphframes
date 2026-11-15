package org.apache.spark.sql.graphframes

import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.storage.StorageLevel

object GraphFramesConf {
  private val USE_LOCAL_CHECKPOINTS =
    SQLConf
      .buildConf("spark.graphframes.useLocalCheckpoints")
      .doc(""" Tells the connected components algorithm to use local checkpoints (default: "false").
          | If set to "true", iterative algorithm will use the checkpointing mechanism to the persistent storage.
          | Local checkpoints are faster but can make the whole job less prone to errors.
          | @note This option may become default "true" in the future.
          |""".stripMargin)
      .version("0.9.3")
      .booleanConf
      .createOptional

  private val USE_LABELS_AS_COMPONENTS =
    SQLConf
      .buildConf("spark.graphframes.useLabelsAsComponents")
      .doc(""" Tells the connected components algorithm to use (default: "true") labels as components in the output
          | DataFrame. If set to "false", randomly generated labels with the data type LONG will returned.
          |""".stripMargin)
      .version("0.9.0")
      .booleanConf
      .createOptional

  private val CONNECTED_COMPONENTS_ALGORITHM =
    SQLConf
      .buildConf("spark.graphframes.connectedComponents.algorithm")
      .doc(""" Sets the connected components algorithm to use (default: "graphframes"). Supported algorithms
          |   - "two_phase": Uses alternating large star and small star iterations proposed in
          |     [[http://dx.doi.org/10.1145/2670979.2670997 Connected Components in MapReduce and Beyond]]
          |   - "randomized_contraction": Uses randomized algorithm proposed in
          |     [[https://arxiv.org/pdf/1802.09478 In-database connected component analysis]]
          |   - "graphframes": Deprecated alias for "two_phase"
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

  private val CONNECTED_COMPONENTS_IN_MEMORY_FINISH_ENABLED =
    SQLConf
      .buildConf("spark.graphframes.connectedComponents.inMemoryFinish.enabled")
      .doc(""" Enables the opt-in in-memory Union-Find finish for the two-phase connected components
          | algorithm (default: false). When true, after the leaf-pruning attempt the algorithm may
          | finish on the driver if residual edge-count and superset-budget gates pass.""".stripMargin)
      .version("0.13.0")
      .booleanConf
      .createOptional

  private val CONNECTED_COMPONENTS_IN_MEMORY_FINISH_EDGE_FRACTION =
    SQLConf
      .buildConf("spark.graphframes.connectedComponents.inMemoryFinish.edgeFraction")
      .doc(""" Residual edge-count gate as a fraction of the post-preparation edge count (default: 0.01).
          | Used only when in-memory finish is enabled.""".stripMargin)
      .version("0.13.0")
      .doubleConf
      .createOptional

  private val CONNECTED_COMPONENTS_IN_MEMORY_FINISH_MAX_SUPERSETS =
    SQLConf
      .buildConf("spark.graphframes.connectedComponents.inMemoryFinish.maxSupersets")
      .doc(""" Maximum distinct provisional set IDs allowed on the driver for in-memory finish
          | (default: 1000000). Used only when in-memory finish is enabled.""".stripMargin)
      .version("0.13.0")
      .longConf
      .createOptional

  private val CONNECTED_COMPONENTS_IN_MEMORY_FINISH_MIN_ITER =
    SQLConf
      .buildConf("spark.graphframes.connectedComponents.inMemoryFinish.minIter")
      .doc(""" Minimum two-phase iterations before in-memory finish may run (default: 2).
          | Used only when in-memory finish is enabled.""".stripMargin)
      .version("0.13.0")
      .intConf
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

  def getUseLabelsAsComponents: Option[Boolean] = get(USE_LABELS_AS_COMPONENTS) match {
    case Some(use) => Some(use.toBoolean)
    case _ => None
  }

  def getUseLocalCheckpoints: Option[Boolean] = get(USE_LOCAL_CHECKPOINTS).map(_.toBoolean)

  def getConnectedComponentsInMemoryFinishEnabled: Option[Boolean] =
    get(CONNECTED_COMPONENTS_IN_MEMORY_FINISH_ENABLED).map(_.toBoolean)

  def getConnectedComponentsInMemoryFinishEdgeFraction: Option[Double] =
    get(CONNECTED_COMPONENTS_IN_MEMORY_FINISH_EDGE_FRACTION).map(_.toDouble)

  def getConnectedComponentsInMemoryFinishMaxSupersets: Option[Long] =
    get(CONNECTED_COMPONENTS_IN_MEMORY_FINISH_MAX_SUPERSETS).map(_.toLong)

  def getConnectedComponentsInMemoryFinishMinIter: Option[Int] =
    get(CONNECTED_COMPONENTS_IN_MEMORY_FINISH_MIN_ITER).map(_.toInt)
}
