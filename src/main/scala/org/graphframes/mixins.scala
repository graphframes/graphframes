package org.graphframes

private[graphframes] trait WithAlgorithmChoice {
  protected val ALGO_GRAPHX = "graphx"
  protected val ALGO_GRAPHFRAMES = "graphframes"
  protected var algorithm: String = ALGO_GRAPHX
  val supportedAlgorithms: Array[String] = Array(ALGO_GRAPHX, ALGO_GRAPHFRAMES)

  def setAlgorithm(value: String): this.type = {
    require(
      supportedAlgorithms.contains(value),
      s"Supported algorithms are {${supportedAlgorithms.mkString(", ")}}, but got $value.")
    algorithm = value
    this
  }

  def getAlgorithm: String = algorithm
}
