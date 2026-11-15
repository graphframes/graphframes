package org.graphframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

private[graphframes] object JobDescription {
  // Mirrors SparkContext.SPARK_JOB_DESCRIPTION, which is private[spark].
  private[graphframes] val JOB_DESCRIPTION_KEY = "spark.job.description"

  /**
   * Runs `body` and afterwards restores the Spark job description (a thread-local property) that
   * the caller had set, so descriptions set inside `body` do not leak into jobs the caller
   * triggers later on the same thread.
   */
  def withRestoredJobDescription[T](spark: SparkSession)(body: => T): T = {
    val sc = spark.sparkContext
    val previousDescription = sc.getLocalProperty(JOB_DESCRIPTION_KEY)
    try {
      body
    } finally {
      sc.setLocalProperty(JOB_DESCRIPTION_KEY, previousDescription)
    }
  }
}

/**
 * Provides support for customizing the Spark job descriptions set by iterative algorithms.
 *
 * Job descriptions are shown in the "Description" column of the Jobs and Stages pages of the
 * Spark UI, making the progress of long iterative runs visible without reading driver logs.
 */
private[graphframes] trait WithJobDescriptionPrefix {

  /** The prefix used when no custom prefix is set, typically the algorithm name. */
  protected def defaultJobDescriptionPrefix: String

  protected var jobDescriptionPrefixOpt: Option[String] = None

  /**
   * Sets a custom prefix for the Spark job descriptions set by this algorithm (default: the
   * algorithm name). Setting distinct prefixes allows telling apart multiple concurrent runs
   * within the same Spark application.
   */
  def setJobDescriptionPrefix(value: String): this.type = {
    jobDescriptionPrefixOpt = Some(value)
    this
  }

  /**
   * Gets the prefix of the Spark job descriptions set by this algorithm.
   */
  def getJobDescriptionPrefix: String =
    jobDescriptionPrefixOpt.getOrElse(defaultJobDescriptionPrefix)
}

private[graphframes] trait WithAlgorithmChoice {
  protected val ALGO_GRAPHX = "graphx"
  protected val ALGO_GRAPHFRAMES = "graphframes"
  protected var algorithm: String = ALGO_GRAPHX
  val supportedAlgorithms: Array[String] = Array(ALGO_GRAPHX, ALGO_GRAPHFRAMES)

  /**
   * Set an algorithm to use. Supported algorithms are "graphx" and "graphframes".
   *
   * @param value
   * @return
   */
  def setAlgorithm(value: String): this.type = {
    require(
      supportedAlgorithms.contains(value),
      s"Supported algorithms are {${supportedAlgorithms.mkString(", ")}}, but got $value.")
    algorithm = value
    this
  }

  def getAlgorithm: String = algorithm
}

private[graphframes] trait WithCheckpointInterval extends Logging {
  protected var checkpointInterval: Int = 2

  /**
   * Sets checkpoint interval in terms of number of iterations (default: 2). Checkpointing
   * regularly helps recover from failures, clean shuffle files, shorten the lineage of the
   * computation graph, and reduce the complexity of plan optimization. As of Spark 2.0, the
   * complexity of plan optimization would grow exponentially without checkpointing. Hence,
   * disabling or setting longer-than-default checkpoint intervals are not recommended. Checkpoint
   * data is saved under `org.apache.spark.SparkContext.getCheckpointDir` with prefix of the
   * algorithm name. If the checkpoint directory is not set, this throws a `java.io.IOException`.
   * Set a nonpositive value to disable checkpointing. This parameter is only used when the
   * algorithm is set to "graphframes". Its default value might change in the future.
   * @see
   *   `org.apache.spark.SparkContext.setCheckpointDir` in Spark API doc
   */
  def setCheckpointInterval(value: Int): this.type = {
    if (value <= 0 || value > 2) {
      logWarn(
        s"Set checkpointInterval to $value. This would blow up the query plan and hang the " +
          "driver for large graphs.")
    }
    checkpointInterval = value
    this
  }

  // python-friendly setter
  private[graphframes] def setCheckpointInterval(value: java.lang.Integer): this.type = {
    setCheckpointInterval(value.toInt)
  }

  /**
   * Gets checkpoint interval.
   */
  def getCheckpointInterval: Int = checkpointInterval
}

private[graphframes] trait WithBroadcastThreshold extends Logging {
  protected var broadcastThreshold: Int = 1000000

  /**
   * Sets a broadcast threshold in propagating component assignments (default: 1,000,000). If a
   * node degree is greater than this threshold at some iteration, its component assignment will
   * be collected and then broadcasted back to propagate the assignment to its neighbors.
   * Otherwise, the assignment propagation is done by a normal Spark join. This parameter is only
   * used when the algorithm is set to "graphframes". If the value is -1, then the skewness
   * problem is left to the Apache Spark AQE optimizer.
   *
   * **WARNING** using a broadcast threshold is non-free! Under the hood it is calling an action,
   * and if a broadcast threshold is set, then AQE is disabled to avoid wrong results! If your
   * graph does not contain gigantic components, it is strongly recommended to set this value to
   * -1. On benchmarks setting it to -1 gains about x5 better results in performance.
   *
   * **WARNING** the current default value is 1,000,000. It is left for backward compatibility
   * only. In the future versions it may be set to -1 as more reasonable for the most real-world
   * cases (e.g., the data deduplication problem).
   */
  def setBroadcastThreshold(value: Int): this.type = {
    broadcastThreshold = value
    this
  }

  // python-friendly setter
  private[graphframes] def setBroadcastThreshold(value: java.lang.Integer): this.type = {
    setBroadcastThreshold(value.toInt)
  }

  /**
   * Gets broadcast threshold in propagating component assignment.
   * @see
   *   [[org.graphframes.lib.ConnectedComponents.setBroadcastThreshold]]
   */
  def getBroadcastThreshold: Int = broadcastThreshold
}

private[graphframes] trait WithIntermediateStorageLevel extends Logging {

  protected var intermediateStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK

  /**
   * Sets storage level for intermediate datasets that require multiple passes (default:
   * ``MEMORY_AND_DISK``).
   */
  def setIntermediateStorageLevel(value: StorageLevel): this.type = {
    intermediateStorageLevel = value
    this
  }

  /**
   * Gets storage level for intermediate datasets that require multiple passes.
   */
  def getIntermediateStorageLevel: StorageLevel = intermediateStorageLevel

}

private[graphframes] trait WithLgNomEntries {
  protected var lgNomEntries: Int = 12

  /**
   * Sets the log2 of nominal entries used by Theta sketch aggregations.
   */
  def setLgNomEntries(value: Int): this.type = {
    require((value >= 4) && (value <= 26), "lgNomEntries must be between 4 and 26")
    lgNomEntries = value
    this
  }

  /**
   * Gets log2 of nominal entries used by Theta sketch aggregations.
   */
  def getLgNomEntries: Int = lgNomEntries
}

private[graphframes] trait WithMaxIter {
  protected var maxIter: Option[Int] = None

  /**
   * The max number of iterations of algorithm to be performed.
   */
  def maxIter(value: Int): this.type = {
    maxIter = Some(value)
    this
  }
}

private[graphframes] trait WithUseLabelsAsComponents {
  protected var useLabelsAsComponents: Boolean = false

  /**
   * Sets whether to use vertex labels as component identifiers (default: false). When true,
   * vertex labels will be used as component identifiers instead of computing connected
   * components.
   */
  def setUseLabelsAsComponents(value: Boolean): this.type = {
    useLabelsAsComponents = value
    this
  }

  /**
   * Gets whether to use vertex labels as component identifiers.
   */
  def getUseLabelsAsComponents: Boolean = useLabelsAsComponents
}

/**
 * Provides support for local checkpoints in Spark computations.
 *
 * Local checkpoints offer a faster alternative to regular checkpoints as they don't require
 * configuration of checkpointDir in persistent storage (like HDFS or S3). While being more
 * performant, local checkpoints are less reliable since they don't survive node failures and the
 * data is not persisted across multiple nodes.
 */
private[graphframes] trait WithLocalCheckpoints {
  protected var useLocalCheckpoints: Boolean = false

  /**
   * Sets whether to use local checkpoints instead of regular checkpoints (default: false). Local
   * checkpoints are faster but less reliable as they don't survive node failures.
   *
   * @param value
   *   true to use local checkpoints, false for regular checkpoints
   * @return
   *   this instance
   */
  def setUseLocalCheckpoints(value: Boolean): this.type = {
    useLocalCheckpoints = value
    this
  }

  /**
   * Gets whether local checkpoints are being used instead of regular checkpoints.
   *
   * @return
   *   true if local checkpoints are enabled, false otherwise
   */
  def getUseLocalCheckpoints: Boolean = useLocalCheckpoints
}

/**
 * Provides support of graph directions for algorithms.
 */
private[graphframes] trait WithDirection {
  protected var isDirected: Boolean = true

  /**
   * Sets should graph be cosidered as directed.
   *
   * @param value
   *   true to handle graph as directed
   * @return
   */
  def setIsDirected(value: Boolean): this.type = {
    isDirected = value
    this
  }

  /**
   * Gets should graph be considered as directed.
   *
   * @return
   *   true if directed
   */
  def getIsDirected: Boolean = isDirected
}

/**
 * Opt-in two-phase connected-components finish that merges remaining provisional component IDs
 * with a driver-side Union-Find once the residual graph is small enough.
 */
private[graphframes] trait WithInMemoryFinish extends Logging {
  protected var inMemoryFinishEnabled: Boolean = false
  protected var inMemoryFinishEdgeFraction: Double = 0.01
  protected var inMemoryFinishMaxSupersets: Long = 1000000L
  protected var inMemoryFinishMinIter: Int = 2

  /**
   * Enables or disables the in-memory Union-Find finish for the two-phase algorithm (default:
   * false). When enabled, after the leaf-pruning attempt the algorithm may collect remaining
   * conflict edges and finish component assignment on the driver if size gates pass.
   */
  def setInMemoryFinishEnabled(value: Boolean): this.type = {
    inMemoryFinishEnabled = value
    this
  }

  /** Gets whether the in-memory Union-Find finish is enabled. */
  def getInMemoryFinishEnabled: Boolean = inMemoryFinishEnabled

  /**
   * Sets the residual edge-count gate as a fraction of the edge count after graph preparation
   * (default: 0.01). In-memory finish is considered only when
   * `edgeCnt <= fraction * initialEdgeCnt`.
   */
  def setInMemoryFinishEdgeFraction(value: Double): this.type = {
    require(
      value >= 0.0 && value <= 1.0,
      s"inMemoryFinishEdgeFraction must be in [0, 1], got $value")
    inMemoryFinishEdgeFraction = value
    this
  }

  /** Gets the residual edge-count fraction of the initial edge count. */
  def getInMemoryFinishEdgeFraction: Double = inMemoryFinishEdgeFraction

  /**
   * Sets the maximum number of distinct provisional set IDs (supersets) allowed on the driver for
   * the in-memory Union-Find finish (default: 1000000). If the distinct count exceeds this
   * budget, the algorithm continues with distributed iterations.
   */
  def setInMemoryFinishMaxSupersets(value: Long): this.type = {
    require(value >= 0L, s"inMemoryFinishMaxSupersets must be >= 0, got $value")
    inMemoryFinishMaxSupersets = value
    this
  }

  /** Gets the maximum number of supersets allowed for the in-memory finish. */
  def getInMemoryFinishMaxSupersets: Long = inMemoryFinishMaxSupersets

  /**
   * Sets the minimum number of two-phase iterations before the in-memory finish may run (default:
   * 2). The finish still requires the prune attempt to have been tried and the edge / superset
   * gates to pass.
   */
  def setInMemoryFinishMinIter(value: Int): this.type = {
    require(value >= 1, s"inMemoryFinishMinIter must be >= 1, got $value")
    inMemoryFinishMinIter = value
    this
  }

  /** Gets the minimum iteration count before the in-memory finish may run. */
  def getInMemoryFinishMinIter: Int = inMemoryFinishMinIter
}
