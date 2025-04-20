package org.graphframes

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

  /**
   * Gets checkpoint interval.
   */
  def getCheckpointInterval: Int = checkpointInterval
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
