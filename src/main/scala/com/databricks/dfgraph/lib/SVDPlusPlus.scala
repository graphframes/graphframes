package com.databricks.dfgraph.lib


import org.apache.spark.graphx.{Edge, lib => graphxlib}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DoubleType, StructField, StructType}

import com.databricks.dfgraph.DFGraph

/**
 * Implementation of the SVD++ algorithm, based on "Factorization Meets the Neighborhood:
 * a Multifaceted Collaborative Filtering Model",
 * available at [[http://public.research.att.com/~volinsky/netflix/kdd08koren.pdf]].
 */
object SVDPlusPlus {

  /**
   * Implement SVD++ based on "Factorization Meets the Neighborhood:
   * a Multifaceted Collaborative Filtering Model",
   * available at [[http://public.research.att.com/~volinsky/netflix/kdd08koren.pdf]].
   *
   * The prediction rule is rui = u + bu + bi + qi*(pu + |N(u)|^^-0.5^^*sum(y)),
   * see the details on page 6.
   *
   * @param conf SVDPlusPlus parameters
   *
   * @return a graph with vertex attributes containing the trained model
   */
  def run(graph: DFGraph, conf: Conf): (DFGraph, Double) = {
    GraphXConversions.checkVertexId(graph)
    val edges = graph.edges.select(DFGraph.SRC, DFGraph.DST, COLUMN_WEIGHT).map {
      case Row(src: Long, dst: Long, w: Double) => Edge(src, dst, w)
    }
    val (gx, res) = graphxlib.SVDPlusPlus.run(edges, conf.toGraphXConf)
    val fullGx = gx.mapVertices { case (vid, w) =>
      Row(vid, w)
    } .mapEdges( e => Row(e.srcId, e.dstId, e.attr))
    val vStruct = StructType(List(
      graph.vertices.schema(DFGraph.ID),
      vField1))
    val eStruct = StructType(List(
      graph.edges.schema(DFGraph.SRC),
      graph.edges.schema(DFGraph.DST),
      field1, field2, field3, field4))
    (GraphXConversions.fromRowGraphX(fullGx, eStruct, vStruct), res)
  }

  /**
   * Configuration parameters for SVD++.
   *
   *
   *
   * @param rank
   * @param maxIters
   * @param minVal
   * @param maxVal
   * @param gamma1
   * @param gamma2
   * @param gamma6
   * @param gamma7
   */
  // TODO(tjh) put some documentation for the parameters.
  case class Conf(
      rank: Int,
      maxIters: Int,
      minVal: Double,
      maxVal: Double,
      gamma1: Double,
      gamma2: Double,
      gamma6: Double,
      gamma7: Double) extends Serializable {

    /**
     * Convenience conversion method.
     */
    private[dfgraph] def toGraphXConf: graphxlib.SVDPlusPlus.Conf = {
      new graphxlib.SVDPlusPlus.Conf(
        rank = rank,
        maxIters = maxIters,
        minVal = minVal,
        maxVal = maxVal,
        gamma1 = gamma1,
        gamma2 = gamma2,
        gamma6 = gamma6,
        gamma7 = gamma7)
    }
  }

  object Conf {

    /**
     * Default settings for the parameters.
     */
    // TODO(tjh) expose as part of the API
    // TODO(tjh) explain the values of the parameters?
    private[dfgraph] val default = new Conf(10, 2, 0.0, 5.0, 0.007, 0.007, 0.005, 0.015)

    /**
     * Convenience conversion method for users of GraphX.
     * @param conf
     */
    def buildFrom(conf: graphxlib.SVDPlusPlus.Conf): Conf = {
      new Conf(
        rank = conf.rank,
        maxIters = conf.maxIters,
        minVal = conf.minVal,
        maxVal = conf.maxVal,
        gamma1 = conf.gamma1,
        gamma2 = conf.gamma2,
        gamma6 = conf.gamma6,
        gamma7 = conf.gamma7)
    }
  }

  private val COLUMN_WEIGHT = "weight"

  private val COLUMN1 = "column1"
  private val COLUMN2 = "column2"
  private val COLUMN3 = "column3"
  private val COLUMN4 = "column4"
  private val VCOLUMN1 = "vcolumn1"

  private val field1 = StructField(COLUMN1, ArrayType(DoubleType), nullable = false)
  private val field2 = StructField(COLUMN2, ArrayType(DoubleType), nullable = false)
  private val field3 = StructField(COLUMN3, DoubleType, nullable = false)
  private val field4 = StructField(COLUMN4, DoubleType, nullable = false)
  private val vField1 = StructField(VCOLUMN1, DoubleType, nullable = false)
}
