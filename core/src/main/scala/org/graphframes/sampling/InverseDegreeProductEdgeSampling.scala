package org.graphframes.sampling

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.pow
import org.graphframes.GraphFrame

/**
 * A sampling strategy that selects edges based on the inverse product of source and destination
 * vertex degrees raised to a given power.
 *
 * This class implements a degree-based probability sampling strategy where the probability of
 * retaining an edge is inversely proportional to the degrees of its source and destination
 * vertices, weighted by a parameter `alpha`. The parameter `alpha` controls the strength of the
 * degree bias in the sampling process:
 *   - When `alpha = 0`, all edges have equal probability of being selected, resulting in uniform
 *     edge sampling.
 *   - When `alpha > 0`, edges connecting lower-degree vertices are more likely to be sampled, as
 *     the sampling probability decreases with increasing vertex degrees.
 *   - As `alpha` approaches 1, the sampling becomes increasingly biased towards edges between
 *     low-degree nodes, effectively down-sampling edges connected to high-degree vertices.
 *
 * @param alpha
 *   A non-negative weight parameter that controls the influence of vertex degrees on edge
 *   sampling probabilities. Must be in the range [0, 1].
 */
private[graphframes] class InverseDegreeProductEdgeSampling(val alpha: Double)
    extends EdgesSamplingStrategy {
  require(alpha >= 0 && alpha <= 1, "alpha must be in (0, 1)")

  private def degreeBasedProb(srcDegree: Column, dstDegree: Column): Column = {
    // division by zero should not be the case:
    // this function is used on triplets,
    // if the vertex is a part of a triplet,
    // it's degree is at least one
    lit(1.0) / (pow(srcDegree, alpha) * pow(dstDegree, alpha))
  }

  override protected def prepare(graph: GraphFrame): Unit = {

    val degrees = graph.degrees
    val newVertices =
      graph.vertices
        .select(col(GraphFrame.ID))
        .join(degrees, Seq(GraphFrame.ID), "left")
    val newEdges = graph.edges.select(col(GraphFrame.SRC), col(GraphFrame.DST))
    preparedGraph = Some(GraphFrame(newVertices, newEdges))
  }

  override val srcColumns: Option[Seq[Column]] = Some(Seq(col("degree")))
  override val dstColumns: Option[Seq[Column]] = Some(Seq(col("degree")))

  override def getProb(
      srcAttrs: Option[Column],
      dstAttrs: Option[Column],
      edgeAttrs: Option[Column]): Column = {
    val srcDegree = srcAttrs.get.getField("degree")
    val dstDegree = dstAttrs.get.getField("degree")
    degreeBasedProb(srcDegree, dstDegree)
  }
}

object InverseDegreeProductEdgeSampling {
  def apply(alpha: Double): InverseDegreeProductEdgeSampling =
    new InverseDegreeProductEdgeSampling(alpha)
}
