package org.graphframes.convolutions

import org.apache.spark.ml.functions.*
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.graphframes.expressions.KMinSampling
import org.graphframes.GraphFrame
import org.graphframes.Logging

class SamplingConvolution extends Serializable with Logging {
  private var graph: GraphFrame = _
  private var featuresCol: String = "embedding"
  private var maxNbrs: Int = 50
  private var useEdgeDirections: Boolean = false
  private var seed: Long = 42L
  private var concatEmbeddings: Boolean = true

  def onGraph(graph: GraphFrame): this.type = {
    this.graph = graph
    this
  }

  def setFeaturesCol(value: String): this.type = {
    featuresCol = value
    this
  }

  def setMaxNbrs(value: Int): this.type = {
    maxNbrs = value
    this
  }

  def setUseEdgeDirections(value: Boolean): this.type = {
    useEdgeDirections = value
    this
  }

  def setSeed(value: Long): this.type = {
    seed = value
    this
  }

  def setConcatEmbeddings(value: Boolean): this.type = {
    concatEmbeddings = value
    this
  }

  def run(): DataFrame = {
    // Min-Hash sampling based on ID + seed
    val preAggs = (if (useEdgeDirections) {
                     graph.edges
                       .select(col(GraphFrame.SRC), col(GraphFrame.DST))
                   } else {
                     graph.edges
                       .select(GraphFrame.SRC, GraphFrame.DST)
                       .union(graph.edges.select(GraphFrame.DST, GraphFrame.SRC))
                       .distinct()
                   })
      .withColumn("hash", xxhash64(col(GraphFrame.DST), lit(seed)))
      .groupBy(col(GraphFrame.SRC).alias(GraphFrame.ID))

    val vertexDtype = graph.vertices.schema(GraphFrame.ID).dataType
    val encoder = KMinSampling.getEncoder(
      graph.vertices.sparkSession,
      vertexDtype,
      Seq(GraphFrame.ID, "hash"))
    val samplingUDF = KMinSampling.fromSparkType(vertexDtype, maxNbrs, encoder)

    val sampledNbrs = preAggs
      .agg(samplingUDF(struct(col(GraphFrame.ID), col("hash"))).alias("nbrs"))
      .select(col(GraphFrame.ID), explode(col("nbrs")).alias("nbr"))

    val joined =
      sampledNbrs.join(
        graph.vertices.select(col(GraphFrame.ID).alias("nbr"), col(featuresCol)),
        Seq("nbr"),
        "left")

    val foldedNbrsFeatures =
      joined.groupBy(GraphFrame.ID).agg(Summarizer.mean(col(featuresCol)).alias("nbr_embedding"))

    val originalAndNbrs = graph.vertices.join(foldedNbrsFeatures, Seq(GraphFrame.ID), "left")

    if (concatEmbeddings) {
      originalAndNbrs.withColumn(
        featuresCol,
        array_to_vector(
          array_union(vector_to_array(col(featuresCol)), vector_to_array(col("nbr_embedding")))))
    } else {
      originalAndNbrs
    }
  }
}
