package com.databricks.dfgraph.lib

import org.apache.spark.graphx.{lib => graphxlib}

import com.databricks.dfgraph.DFGraph

/**
 * Label propagation algorithm.
 */
object LabelPropagation {
  /**
   * Run static Label Propagation for detecting communities in networks.
   *
   * Each node in the network is initially assigned to its own community. At every superstep, nodes
   * send their community affiliation to all neighbors and update their state to the mode community
   * affiliation of incoming messages.
   *
   * LPA is a standard community detection algorithm for graphs. It is very inexpensive
   * computationally, although (1) convergence is not guaranteed and (2) one can end up with
   * trivial solutions (all nodes are identified into a single community).
   * 
   * The resulting edges are the same as the original edges.
   *
   * The resulting vertices have two columns:
   *  - id: the id of the vertex
   *  - component: (same type as vertex id) the id of a vertex in the connected component, used as a unique identifier
   *    for this component.
   * All the other columns from the vertices are dropped.
   *
   * @param graph the graph for which to compute the community affiliation
   * @param maxSteps the number of supersteps of LPA to be performed. Because this is a static
   * implementation, the algorithm will run for exactly this many supersteps.
   *
   * @return a graph with vertex attributes containing the label of community affiliation.
   */
  def run(graph: DFGraph, maxSteps: Int): DFGraph = {
    GraphXConversions.checkVertexId(graph)
    val gx = graphxlib.LabelPropagation.run(graph.cachedGraphX, maxSteps)
    GraphXConversions.fromVertexGraphX(gx, graph, LABEL_ID)
  }

  private val LABEL_ID = "label"

  class Builder private[dfgraph] (graph: DFGraph) extends Arguments {
    private var maxSteps: Option[Int] = None

    def setMaxSteps(i: Int): this.type = {
      maxSteps = Some(i)
      this
    }

    def run(): DFGraph = {
      LabelPropagation.run(
        graph,
        check(maxSteps, "maxSteps"))
    }
  }
}
