package org.graphframes.lib

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame
import org.graphframes.Logging
import org.graphframes.WithCheckpointInterval
import org.graphframes.WithIntermediateStorageLevel
import org.graphframes.WithLocalCheckpoints

import java.io.IOException
import scala.collection.mutable

/**
 * This class implements a distributed algorithm for finding a Maximal Independent Set (MIS) in a
 * graph.
 *
 * An MIS is a set of vertices such that no two vertices in the set are adjacent (i.e., there is
 * no edge between any two vertices in the set), and the set is maximal, meaning that adding any
 * other vertex to the set would violate the independence property. Note that this implementation
 * finds a maximal (but not necessarily maximum) independent set; that is, it ensures no more
 * vertices can be added to the set, but does not guarantee that the set has the largest possible
 * number of vertices among all possible independent sets in the graph.
 *
 * The algorithm implemented here is based on the paper: Ghaffari, Mohsen. "An improved
 * distributed algorithm for maximal independent set." Proceedings of the twenty-seventh annual
 * ACM-SIAM symposium on Discrete algorithms. Society for Industrial and Applied Mathematics,
 * 2016.
 *
 * Each Ghaffari round is executed in a fused, shuffle-minimal way:
 *   - a single pass over the frozen (symmetrized, de-duplicated, src-partitioned) edge set
 *     scatters `(p, nominated)` of every active vertex to its neighbours and computes both the
 *     effective degree `d(v)` and the "has nominated neighbour" flag in one aggregation;
 *   - the aggregated messages are left-joined back onto the per-vertex state; a vertex that
 *     received no messages has no active neighbour and joins the MIS regardless of its draw,
 *     which subsumes the classic isolated-vertices handling;
 *   - the removed set (elected vertices and their neighbours) is deliberately not de-duplicated
 *     and is consumed by a single anti-join;
 *   - vertices elected in a round are appended to an append-only log. A vertex can be elected at
 *     most once (it leaves the active set in the same round), so the per-round sets are pairwise
 *     disjoint and the final result is their union.
 *
 * Following the paper, the nomination of a vertex is drawn against its *current* probability
 * `p_t`; only afterwards is the probability advanced to `p_{t+1}`, which is what the next round
 * reads. The draw is materialized together with the per-vertex state so that every consumer
 * within a round reads the same stable values.
 *
 * Note: This is a randomized, non-deterministic algorithm. The result may vary between runs even
 * if a fixed random seed is provided because how Apache Spark works.
 *
 * @param graph
 */
class MaximalIndependentSet private[graphframes] (private val graph: GraphFrame)
    extends Serializable
    with WithIntermediateStorageLevel
    with WithCheckpointInterval
    with WithLocalCheckpoints {
  def run(seed: Long): DataFrame = {
    MaximalIndependentSet.run(
      graph,
      checkpointInterval,
      useLocalCheckpoints,
      intermediateStorageLevel,
      seed)
  }
}

object MaximalIndependentSet extends Serializable with Logging {
  private val probCol = "prob"
  private val degCol = "effectiveDegree"
  private val isNominated = "isNominated"
  private val hasNominatedNbrCol = "hasNominatedNbr"
  private val isElectedCol = "isElected"
  // Throw-away key of the removed frame. It only exists to keep the two sides of the removal
  // anti-join on distinct column names: both sides derive from the same per-vertex state.
  private val removedKeyCol = "misRemovedId"

  /**
   * Materializes one generation of the per-vertex state (id, p, nominated) and truncates its
   * lineage.
   *
   * Two invariants make the algorithm correct and scalable:
   *   - the random nomination drawn in the state projection must be evaluated exactly once per
   *     round: `rand()` is a per-row and per-execution expression, so a re-evaluated draw (e.g.,
   *     after a cache eviction under a duplicated plan) could yield inconsistent nominations
   *     within the same round;
   *   - the loop-carried state must stay a (small) leaf: every round embeds the state subtree
   *     several times (gather, vertex program, removal), so without per-round truncation the
   *     query plan grows exponentially.
   *
   * Therefore the state is checkpointed (locally, which is eager by definition) on every round;
   * when local checkpoints are disabled, a reliable checkpoint is written every
   * `checkpointInterval` iterations as a fault-tolerance recovery point, and local checkpointing
   * is still used between them purely to truncate the lineage.
   */
  private def materializeState(
      state: DataFrame,
      iteration: Int,
      checkpointInterval: Int,
      useLocalCheckpoints: Boolean): DataFrame = {
    if (!useLocalCheckpoints && checkpointInterval > 0 && (iteration % checkpointInterval == 0)) {
      state.checkpoint(eager = true)
    } else {
      state.localCheckpoint(eager = true)
    }
  }

  private def run(
      graph: GraphFrame,
      checkpointInterval: Int,
      useLocalCheckpoints: Boolean,
      storageLevel: StorageLevel,
      seed: Long): DataFrame = {
    val spark = graph.vertices.sparkSession

    if (!useLocalCheckpoints && spark.sparkContext.getCheckpointDir.isEmpty) {
      // Spark-Connect workaround
      spark.sparkContext
        .setCheckpointDir(spark.conf
          .getOption("spark.checkpoint.dir") match {
          case Some(d) => d
          case None =>
            throw new IOException(
              "Checkpoint directory is not set. Please set it first using sc.setCheckpointDir()" +
                "or by specifying the conf 'spark.checkpoint.dir'.")
        })
    }

    val rng = new util.Random(seed)

    var i = 0
    var converged = false
    // append-only elected log: one frame per round. A vertex can be elected at most once (it
    // leaves the active set in the same round), so the per-round id sets are pairwise
    // disjoint, and the final result is their union without any deduplication.
    val electedLog = mutable.ArrayBuffer.empty[DataFrame]

    // randomized algorithms are not working with AQE well
    val originalAQE = spark.conf.get("spark.sql.adaptive.enabled")
    try {
      spark.conf.set("spark.sql.adaptive.enabled", "false")

      // The symmetrized and de-duplicated edge set is the only edge data the algorithm ever
      // touches, and it is persisted exactly once. It is hash-partitioned and sorted by `src`
      // so that every src-keyed join below sees co-partitioned input and skips the shuffle on
      // the (large) edge side. De-duplication is required here: a duplicated edge would
      // inflate the effective-degree sum.
      val edges = graph.edges
        .select(GraphFrame.SRC, GraphFrame.DST)
        .union(
          graph.edges.select(
            col(GraphFrame.DST).alias(GraphFrame.SRC),
            col(GraphFrame.SRC).alias(GraphFrame.DST)))
        .filter(col(GraphFrame.SRC) =!= col(GraphFrame.DST))
        .distinct()
        .repartition(col(GraphFrame.SRC))
        .sortWithinPartitions(col(GraphFrame.SRC))
        .persist(storageLevel)

      // Per-vertex state: (id, p_t, nominated_t). The nomination is drawn against the current
      // probability p_t when the state is materialized (see materializeState).
      var state = materializeState(
        graph.vertices
          .select(
            col(GraphFrame.ID),
            lit(0.5).cast(DoubleType).alias(probCol),
            (rand(rng.nextLong()) <= lit(0.5)).alias(isNominated))
          .repartition(col(GraphFrame.ID)),
        iteration = -1,
        checkpointInterval = checkpointInterval,
        useLocalCheckpoints = useLocalCheckpoints)

      while (!converged) {
        // 1) gather: a single shuffle-free pass over the frozen edges. It scatters
        //    (p_t, nominated_t) of every *active* source to its neighbours, and one
        //    aggregation computes both d(v) = sum of p over active neighbours and
        //    hasNominatedNbr(v) = whether at least one active neighbour is nominated.
        //    The inner join against the active state is the edge contraction: a removed
        //    vertex is absent from the state, hence sends nothing.
        val gathered = edges
          .join(
            state.select(col(GraphFrame.ID), col(probCol), col(isNominated)),
            col(GraphFrame.ID) === col(GraphFrame.SRC))
          .groupBy(GraphFrame.DST)
          .agg(
            sum(col(probCol)).alias(degCol),
            bool_or(col(isNominated)).alias(hasNominatedNbrCol))

        // 2) vertex program: the aggregated messages are left-joined back onto the state. A
        //    vertex that received no messages has d = NULL, i.e. no active neighbour, and
        //    joins the MIS regardless of its draw; this replaces the separate
        //    isolated-vertices anti-join. Per the paper, the nomination is compared against
        //    p_t; only afterwards is p advanced to p_{t+1}, which is what the next round
        //    reads. The p update and the election flag are local projections.
        val candidates = state
          .join(gathered, col(GraphFrame.ID) === col(GraphFrame.DST), "left")
          .withColumn(
            isElectedCol,
            col(degCol).isNull ||
              (col(isNominated) && !coalesce(col(hasNominatedNbrCol), lit(false))))
          .withColumn(
            probCol,
            when(col(degCol) >= lit(2), col(probCol) / lit(2.0)).otherwise(
              when(lit(2) * col(probCol) <= lit(0.5), lit(2) * col(probCol))
                .otherwise(lit(0.5))))
          .select(GraphFrame.ID, probCol, isElectedCol)
          .persist(storageLevel)

        // The round is materialized exactly once, here; everything below is a cheap
        // filter or join over these frames. This also freezes the elected ids of the round.
        val elected = candidates
          .filter(col(isElectedCol))
          .select(col(GraphFrame.ID))
          .persist(storageLevel)
        elected.count()
        electedLog += elected

        // 3) removal: removed = elected ∪ N(elected), deliberately *not* de-duplicated: it
        //    only feeds an anti-join, which is insensitive to duplicate right-hand rows.
        //    N(elected) is computed with a src-keyed join over the frozen edges (the edge
        //    table is symmetric, so this yields exactly the neighbours), so the
        //    co-partitioned edge side is not shuffled.
        val neighborsOfElected = edges
          .join(
            elected.select(col(GraphFrame.ID).alias(removedKeyCol)),
            col(GraphFrame.SRC) === col(removedKeyCol))
          .select(col(GraphFrame.DST).alias(removedKeyCol))
        val removed =
          neighborsOfElected.union(elected.select(col(GraphFrame.ID).alias(removedKeyCol)))

        // 4) next state: survivors, drawing the next round's nomination against p_{t+1} at
        //    materialization time. The final repartition is free when the anti-join already
        //    produced this layout; otherwise it pins the co-partitioned layout for the next
        //    round's gather join.
        val nextState = candidates
          .filter(!col(isElectedCol))
          .join(removed, col(GraphFrame.ID) === col(removedKeyCol), "left_anti")
          .select(col(GraphFrame.ID), col(probCol))
          .withColumn(isNominated, rand(rng.nextLong()) <= col(probCol))
          .repartition(col(GraphFrame.ID))

        val newState = materializeState(
          nextState,
          iteration = i,
          checkpointInterval = checkpointInterval,
          useLocalCheckpoints = useLocalCheckpoints)

        // one job serves both the convergence check and the logging
        val verticesLeft = newState.count()

        candidates.unpersist()
        state.unpersist()
        state = newState

        // algorithm stops if no more vertex left
        converged = verticesLeft == 0L

        logInfo(s"iteration $i finished, vertices left: $verticesLeft")
        i += 1
      }

      state.unpersist()
      edges.unpersist(true)

      val mis = electedLog
        .reduce(_.union(_))
        .select(col(GraphFrame.ID))
        .persist(storageLevel)
      // materialize
      mis.count()
      resultIsPersistent()
      electedLog.foreach(_.unpersist())

      mis
    } finally {
      // Restore original AQE setting
      spark.conf.set("spark.sql.adaptive.enabled", originalAQE)
    }
  }
}
