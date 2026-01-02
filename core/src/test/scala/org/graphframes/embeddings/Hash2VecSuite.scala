package org.graphframes.embeddings

import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.graphframes.GraphFrameTestSparkContext
import org.graphframes.SparkFunSuite
import org.scalatest.BeforeAndAfterAll

import scala.util.Random

class Hash2VecSuite extends SparkFunSuite with GraphFrameTestSparkContext with BeforeAndAfterAll {
  private var longSequences: DataFrame = _
  private var stringSequences: DataFrame = _
  private var uniqueElementsCnt: Int = _

  private def approxEqual(left: Double, right: Double, err: Double = 1e-6.toDouble): Boolean =
    math.abs(left - right) < err

  override def beforeAll(): Unit = {
    super.beforeAll()

    val rng = new Random(42L)

    val sequences =
      (1 to 100).map(idx => (idx, (1 to 20).map(_ => rng.nextInt(30).toLong).toSeq)).toSeq

    uniqueElementsCnt = sequences.flatMap(f => f._2).distinct.length

    val strSequences = sequences.map(f => (f._1, f._2.map(_.toString()))).toSeq

    longSequences = spark.createDataFrame(sequences).toDF("id", "seq")
    stringSequences = spark.createDataFrame(strSequences).toDF("id", "seq")
  }

  test("hash2vec long input") {
    val hash2vecResults = new Hash2Vec().setSequenceCol("seq").run(longSequences)
    assert(hash2vecResults.schema.fields.length === 2)
    assert(hash2vecResults.schema.fields.map(_.name).toSeq === Seq("id", "vector"))
    assert(hash2vecResults.schema("id").dataType === LongType)
    assert(hash2vecResults.schema("vector").dataType === VectorType)
    val collected = hash2vecResults.collect()
    assert(collected.length === uniqueElementsCnt)
  }

  test("hash2vec string input") {
    val hash2vecResults = new Hash2Vec().setSequenceCol("seq").run(stringSequences)
    assert(hash2vecResults.schema.fields.length === 2)
    assert(hash2vecResults.schema.fields.map(_.name).toSeq === Seq("id", "vector"))
    assert(hash2vecResults.schema("id").dataType === StringType)
    assert(hash2vecResults.schema("vector").dataType === VectorType)
    val collected = hash2vecResults.collect()
    assert(collected.length === uniqueElementsCnt)
  }

  test("hash2vec reproducable with seed") {
    val hash2vecResults =
      new Hash2Vec().setSequenceCol("seq").setHashingSeed(42).run(longSequences)
    val hash2vecResults2 =
      new Hash2Vec().setSequenceCol("seq").setHashingSeed(42).run(longSequences)
    val hash2vecResults3 =
      new Hash2Vec().setSequenceCol("seq").setHashingSeed(43).run(longSequences)

    val shouldMatch = hash2vecResults
      .withColumnRenamed("vector", "left")
      .join(hash2vecResults2, Seq("id"), "inner")

    assert(shouldMatch.count() === hash2vecResults.count())
    assert(shouldMatch.filter(col("left") =!= col("vector")).count() === 0)

    val shouldNotMatch = hash2vecResults
      .withColumnRenamed("vector", "left")
      .join(hash2vecResults3, Seq("id"), "inner")

    assert(shouldNotMatch.count() === uniqueElementsCnt)
    assert(shouldNotMatch.filter(col("left") =!= col("vector")).count() > 0)
  }

  test("hash2vec L2") {
    val hash2vecResults =
      new Hash2Vec()
        .setDoNormalization(true, false)
        .setSequenceCol("seq")
        .run(longSequences)
        .collect()

    def naiveL2norm(vector: DenseVector): Double = {
      val squaredSum = vector.values.map(el => el * el).sum[Double]
      math.sqrt(squaredSum)
    }

    assert(
      hash2vecResults
        .map(r => r.getAs[DenseVector](1))
        .map(v => naiveL2norm(v))
        .forall(f => approxEqual(f, 1.0)))
  }
}
