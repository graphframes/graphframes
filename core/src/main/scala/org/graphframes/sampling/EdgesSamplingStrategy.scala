package org.graphframes.sampling

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.rand
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.functions.sum
import org.graphframes.GraphFrame
import org.graphframes.WithIntermediateStorageLevel

/**
 * An abstract class that defines a strategy for sampling edges in a graph. This class provides
 * the foundational structure and methods needed to implement various edge sampling techniques
 * based on vertex and edge attributes. It supports probabilistic sampling where each edge's
 * likelihood of being retained in the sample can be influenced by its associated source vertex,
 * destination vertex, and edge-specific attributes.
 *
 * The class manages internal state including the prepared graph, number of edges, and optional
 * attribute columns for source, destination, and edge data. It also handles resource management
 * through persistence and unpersistence of intermediate DataFrames to optimize memory usage.
 *
 * Concrete implementations must define the `getProb` method to compute the probability of keeping
 * an edge based on the provided attributes and a random seed. The sampling process involves
 * estimating a normalization factor to adjust probabilities and achieve the desired sampling
 * ratio.
 *
 * Usage involves preparing the sampler with a GraphFrame, invoking the sampling method with
 * appropriate parameters such as seed, threshold, and sample size for normalization estimation,
 * and finally closing the sampler to release resources.
 */
abstract class EdgesSamplingStrategy extends Serializable with WithIntermediateStorageLevel {
  private val edgeKeepProbability = "_edgeKeepProbability"
  private var numEdges: Long = _

  /**
   * Represents an optional sequence of columns associated with the source vertices for edge
   * sampling strategies.
   *
   * This can be used to specify additional attributes of the source vertices that might influence
   * the edge sampling process. It is optional and is initialized to `None` by default.
   */
  val srcColumns: Option[Seq[Column]] = None

  /**
   * Specifies optional destination vertex attributes to be used during the edge sampling process.
   * These attributes may influence the sampling probability or logic for each edge. In this
   * implementation, the default value is None, indicating that no destination attributes are
   * provided for consideration.
   */
  val dstColumns: Option[Seq[Column]] = None

  /**
   * Represents optional edge-specific columns that might be considered during the edge sampling
   * process. These columns can contain additional attributes or metadata for the edges in the
   * graph.
   */
  val edgeColumns: Option[Seq[Column]] = None

  protected var preparedGraph: Option[GraphFrame] = None

  /**
   * Prepares the sampling strategy by initializing the internal state with the provided graph if
   * not already initialized.
   *
   * @param graph
   *   the input GraphFrame to be used for preparation
   * @return
   *   Unit, as this method initializes the internal state without returning a value
   */
  protected def prepare(graph: GraphFrame): Unit = {
    if (preparedGraph.isEmpty) { preparedGraph = Some(graph) }
  }

  def prepareSampler(graph: GraphFrame): this.type = {
    numEdges = graph.edges.count()
    prepare(graph)
    preparedGraph = Some(preparedGraph.get.persist(intermediateStorageLevel))
    this
  }

  /**
   * Computes the probability of keeping an edge based on the provided attributes and a random
   * seed.
   *
   * @param seed
   *   the random seed used to introduce deterministic randomness in the probability calculation
   * @param srcAttrs
   *   an optional column containing the attributes of the source vertex
   * @param dstAttrs
   *   an optional column containing the attributes of the destination vertex
   * @param edgeAttrs
   *   an optional column containing the attributes of the edge
   * @return
   *   a Column representing the probability of keeping the edge
   */
  def getProb(
      srcAttrs: Option[Column],
      dstAttrs: Option[Column],
      edgeAttrs: Option[Column]): Column

  /**
   * Estimates the normalization factor used in edge sampling to adjust for varying edge
   * probabilities. This factor is computed as the sum of the edge keep probabilities over a
   * sampled subset of edges adjusted by the sampleSize (fraction of samples).
   *
   * @param edges
   *   the DataFrame of edges containing the edge keep probabilities
   * @param sampleSize
   *   the fraction of edges to sample for estimating the normalization factor
   * @return
   *   the estimated normalization factor, with a minimum value to avoid division by zero
   */
  protected def estimateNormalizeFactor(
      edges: DataFrame,
      sampleSize: Double,
      seed: Long): Double = {
    val estimatedSum = edges
      .select(edgeKeepProbability)
      .sample(sampleSize, seed)
      .select(sum(edgeKeepProbability))
      .head()
      .getDouble(0)
    Math.max(estimatedSum, 0.000001) / sampleSize
  }

  /**
   * Samples edges from the prepared graph based on computed weights and a given threshold. The
   * sampling process uses a normalization factor estimated from a subset of edges to ensure the
   * desired sampling ratio is achieved. Based on the estimated normalization factor sampling is
   * done via: p = w_i * threshold * numEdges / sum_i(w_i)
   *
   * @param seed
   *   the random seed used for sampling to ensure reproducibility
   * @param threshold
   *   the target fraction of edges to retain in the sampled graph
   * @param zSampleSize
   *   the fraction of edges to sample when estimating the normalization factor
   * @return
   *   a DataFrame containing the sampled edges with columns for source and destination IDs
   */
  def sampleEdges(seed: Long, threshold: Double, zSampleSize: Double): DataFrame = {
    require(Math.max(0.0, threshold) == Math.min(threshold, 1.0), "threshold should be in (0, 1]")
    require(
      Math.max(0.0, threshold) == Math.min(threshold, 1.0),
      "z-estimation sample size should be in (0, 1]")
    if (this.preparedGraph.isEmpty) {
      throw new IllegalStateException("GraphFrame is not prepared for sampling")
    }
    val preparedGraph = this.preparedGraph.get

    val sources = srcColumns match {
      case Some(cols) =>
        preparedGraph.vertices.select(col(GraphFrame.ID), struct(cols: _*).alias("src_attrs"))
      case None => preparedGraph.vertices.select(col(GraphFrame.ID))
    }

    val destinations = dstColumns match {
      case Some(cols) =>
        preparedGraph.vertices.select(col(GraphFrame.ID), struct(cols: _*).alias("dst_attrs"))
      case None => preparedGraph.vertices.select(col(GraphFrame.ID))
    }

    val edges = edgeColumns match {
      case Some(cols) => preparedGraph.edges.select(struct(cols: _*).alias("edge_attrs"))
      case None => preparedGraph.edges.select(GraphFrame.SRC, GraphFrame.DST)
    }

    val triplets = sources
      .join(edges, col(GraphFrame.ID) === col(GraphFrame.SRC))
      .drop(GraphFrame.ID)
      .join(destinations, col(GraphFrame.ID) === col(GraphFrame.DST))
      .drop(GraphFrame.ID)
      .withColumn(
        edgeKeepProbability,
        getProb(
          srcColumns match {
            case Some(_) => Some(col("src_attrs"))
            case None => None
          },
          dstColumns match {
            case Some(_) => Some(col("dst_attrs"))
            case None => None
          },
          edgeColumns match {
            case Some(_) => Some(col("edge_attrs"))
            case None => None
          }))

    val zFactor = estimateNormalizeFactor(triplets, zSampleSize, seed)

    val probs = triplets
      .withColumn(
        "prob",
        col(edgeKeepProbability) * lit(threshold) * lit(numEdges) / lit(zFactor))

    probs
      .filter(col("prob") >= rand(seed))
      .select(GraphFrame.SRC, GraphFrame.DST)
  }

  /**
   * Closes the sampling strategy by unpersisting the prepared graph, freeing up storage
   * resources. This method should be called after sampling is complete to ensure proper resource
   * cleanup.
   *
   * @return
   *   Unit, as this method performs a side effect of unpersisting the graph
   */
  def close(): Unit = {
    val _ = preparedGraph.get.unpersist(true)
  }
}
