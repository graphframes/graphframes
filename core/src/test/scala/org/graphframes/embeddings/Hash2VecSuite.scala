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

  test("hash2vec safe L2") {
    val hash2vecResults = new Hash2Vec()
      .setDoNormalization(true, true)
      .setEmbeddingsDim(128)
      .setSequenceCol("seq")
      .run(longSequences)
      .collect()

    assert(hash2vecResults.forall(r => r.getAs[DenseVector](1).size === 129))
  }

  test("constant decay") {
    val hash2vecResults = new Hash2Vec()
      .setDecayFunction("constant")
      .setSequenceCol("seq")
      .run(longSequences)
    hash2vecResults.write.mode("overwrite").format("noop").save()
  }

  test("context longer than sequence") {
    val hash2vecResults = new Hash2Vec()
      .setSequenceCol("seq")
      .setContextSize(30)
      .run(longSequences)
    hash2vecResults.write.mode("overwrite").format("noop").save()
  }

  test("PagedMatrixDouble helper - page extension") {
    val dim = 50
    val matrix = new Hash2Vec.PagedMatrixDouble(dim)
    // Allocate enough vectors to force a second page (PAGE_SIZE = 65536)
    // We'll allocate two pages' worth to be sure.
    // For simplicity, allocate 2 * PAGE_SIZE vectors.
    val PAGE_SIZE = 1 << 16 // 65536
    val totalVectors = 2 * PAGE_SIZE
    val ids = (0 until totalVectors).map { _ =>
      matrix.allocateVector()
    }
    // IDs should be 0,1,2,...,totalVectors-1
    assert(ids.toSeq == (0 until totalVectors).toSeq)
    // Check that internal pages count grew
    // Since internal structure is private, we just verify no exception occurred.
  }

  test("PagedMatrixDouble helper - add and retrieve") {
    val dim = 10
    val matrix = new Hash2Vec.PagedMatrixDouble(dim)
    val id0 = matrix.allocateVector()
    val id1 = matrix.allocateVector()
    assert(id0 == 0)
    assert(id1 == 1)

    // Add values to vector 0 at different offsets
    matrix.add(id0, 0, 5.0)
    matrix.add(id0, 3, 2.0)
    matrix.add(id0, 9, -1.0)

    // Add values to vector 1
    matrix.add(id1, 0, 10.0)
    matrix.add(id1, 5, 3.0)

    // Retrieve and verify
    val vec0 = matrix.getVector(id0)
    assert(vec0.length == dim)
    assert(vec0(0) == 5.0)
    assert(vec0(3) == 2.0)
    assert(vec0(9) == -1.0)
    // Other positions should be zero
    (0 until dim).filterNot(idx => idx == 0 || idx == 3 || idx == 9).foreach { idx =>
      assert(vec0(idx) == 0.0)
    }

    val vec1 = matrix.getVector(id1)
    assert(vec1.length == dim)
    assert(vec1(0) == 10.0)
    assert(vec1(5) == 3.0)
    (0 until dim).filterNot(idx => idx == 0 || idx == 5).foreach { idx =>
      assert(vec1(idx) == 0.0)
    }
  }

  test("PagedMatrixDouble helper - cross-page addressing") {
    val dim = 20
    val matrix = new Hash2Vec.PagedMatrixDouble(dim)
    val PAGE_SIZE = 1 << 16
    // Allocate vectors up to the end of first page
    for (_ <- 0 until PAGE_SIZE) matrix.allocateVector()
    val firstPageLastId = PAGE_SIZE - 1
    // Allocate first vector of second page
    val secondPageFirstId = matrix.allocateVector()
    assert(secondPageFirstId == PAGE_SIZE)

    // Add to the last vector of first page
    matrix.add(firstPageLastId, 0, 100.0)
    matrix.add(firstPageLastId, dim - 1, 200.0)

    // Add to the first vector of second page
    matrix.add(secondPageFirstId, 0, 300.0)
    matrix.add(secondPageFirstId, 10, 400.0)

    // Retrieve and verify
    val vecFirstPage = matrix.getVector(firstPageLastId)
    assert(vecFirstPage(0) == 100.0)
    assert(vecFirstPage(dim - 1) == 200.0)
    (1 until dim - 1).foreach { idx =>
      assert(vecFirstPage(idx) == 0.0)
    }

    val vecSecondPage = matrix.getVector(secondPageFirstId)
    assert(vecSecondPage(0) == 300.0)
    assert(vecSecondPage(10) == 400.0)
    (1 until dim).filterNot(_ == 10).foreach { idx =>
      assert(vecSecondPage(idx) == 0.0)
    }
  }
}
