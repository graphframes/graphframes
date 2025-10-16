package org.graphframes.sampling

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.rand

/**
 * A concrete implementation of the `EdgesSamplingStrategy` that performs random edge sampling
 * based solely on a specified random seed.
 *
 * This sampling strategy does not rely on source vertex attributes, destination vertex
 * attributes, or edge attributes to determine edge retention probabilities. Instead, a random
 * probability is generated for each edge, which can be used to retain or discard the edge during
 * the sampling process.
 */
private[graphframes] class RandomEdgesSampling extends EdgesSamplingStrategy {
  override def getProb(
      srcAttrs: Option[Column],
      dstAttrs: Option[Column],
      edgeAttrs: Option[Column]): Column = rand()
}

object RandomEdgesSampling {
  def apply(): RandomEdgesSampling = new RandomEdgesSampling()
}
