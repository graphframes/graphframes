package org.graphframes.embeddings

import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.hash.Murmur3_x86_32.*
import org.apache.spark.unsafe.types.UTF8String
import org.graphframes.GraphFramesUnsupportedVertexTypeException
import org.graphframes.rw.RandomWalkBase

import scala.jdk.CollectionConverters.*
import scala.reflect.ClassTag

class Hash2Vec extends Serializable {
  private def decayGaussian(d: Int, sigma: Double): Double = {
    math.exp(-(d * d) / (sigma * sigma))
  }
  private val possibleDecayFunctions: Seq[String] = Seq("gaussian", "constant")

  private var contextSize: Int = 5
  private var numPartitions: Int = 5
  private var embeddingsDim: Int = 256
  private var sequenceCol: String = RandomWalkBase.rwColName
  private var decayFunction: String = "gaussian"
  private var gaussianSigma: Double = 1.0
  private var hashingSeed: Int = 42
  private var signHashingSeed: Int = 18

  def setContextSize(value: Int): this.type = {
    contextSize = value
    this
  }

  def setNumPartitions(value: Int): this.type = {
    numPartitions = value
    this
  }

  def setEmbeddingsDim(value: Int): this.type = {
    embeddingsDim = value
    this
  }

  def setSequenceCol(value: String): this.type = {
    sequenceCol = value
    this
  }

  def setDecayFunction(value: String): this.type = {
    val sep = ", "
    require(
      possibleDecayFunctions.contains(value),
      s"supported functions: ${possibleDecayFunctions.mkString(sep)}")
    decayFunction = value
    this
  }

  def setGaussianSigma(value: Double): this.type = {
    gaussianSigma = value
    this
  }

  def setHashingSeed(value: Int): this.type = {
    hashingSeed = value
    this
  }

  def setSignHashSeed(value: Int): this.type = {
    signHashingSeed = value
    this
  }

  private def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  private var valueHash: (Any) => Int = _
  private var signHash: (Any) => Int = _
  private var weightFunction: (Int) => Double = _

  private def hashFunc(term: Any, seed: Int): Int = {
    term match {
      case null => seed
      case b: Boolean => hashInt(if (b) 1 else 0, seed)
      case b: Byte => hashInt(b.toInt, seed)
      case s: Short => hashInt(s.toInt, seed)
      case i: Int => hashInt(i, seed)
      case l: Long => hashLong(l, seed)
      case f: Float => hashInt(java.lang.Float.floatToIntBits(f), seed)
      case d: Double => hashLong(java.lang.Double.doubleToLongBits(d), seed)
      case s: String =>
        val utf8 = UTF8String.fromString(s)
        hashUnsafeBytes(utf8.getBaseObject, utf8.getBaseOffset, utf8.numBytes(), seed)
      case _ =>
        throw new GraphFramesUnsupportedVertexTypeException(
          "Hashing2vec with murmur3 algorithm does not " +
            s"support type ${term.getClass.getCanonicalName} of input data.")
    }
  }

  def run(data: DataFrame): DataFrame = {
    val spark = data.sparkSession
    require(data.schema(sequenceCol).dataType.isInstanceOf[ArrayType], "sequence should be array")
    val elDataType = data.schema(sequenceCol).dataType.asInstanceOf[ArrayType].elementType

    weightFunction = decayFunction match {
      case "gaussian" => (d: Int) => decayGaussian(d, gaussianSigma)
      case "constant" => (_: Int) => 1.0
      case _ => throw new RuntimeException(s"unsupported decay functions $decayFunction")
    }

    valueHash = (el: Any) => nonNegativeMod(hashFunc(el, hashingSeed), embeddingsDim)
    signHash = (el: Any) => nonNegativeMod(hashFunc(el, signHashingSeed), 2)

    val (rowRDD, schema) = elDataType match {
      case _: StringType =>
        (
          runTyped[String](data).map(f => Row(f._1, f._2)),
          StructType(Seq(StructField("id", StringType), StructField("vector", VectorType))))
      case _: ByteType =>
        (
          runTyped[Byte](data).map(f => Row(f._1, f._2)),
          StructType(Seq(StructField("id", ByteType), StructField("vector", VectorType))))
      case _: ShortType =>
        (
          runTyped[Short](data).map(f => Row(f._1, f._2)),
          StructType(Seq(StructField("id", ShortType), StructField("vector", VectorType))))
      case _: IntegerType =>
        (
          runTyped[Int](data).map(f => Row(f._1, f._2)),
          StructType(Seq(StructField("id", IntegerType), StructField("vector", VectorType))))
      case _: LongType =>
        (
          runTyped[Long](data).map(f => Row(f._1, f._2)),
          StructType(Seq(StructField("id", LongType), StructField("vector", VectorType))))
      case _ =>
        throw new GraphFramesUnsupportedVertexTypeException(
          s"Hash2vec supports only string or numeric types of elements but gor ${elDataType.toString()}")
    }

    spark.createDataFrame(rowRDD, schema).groupBy("id").agg(Summarizer.sum(col("vector")))
  }

  private def runTyped[T: ClassTag](data: DataFrame): RDD[(T, DenseVector)] = {
    data
      .select(col(sequenceCol))
      .rdd
      .map(_.getAs[Seq[T]](0))
      .repartition(numPartitions)
      .mapPartitions(processPartition[T])
  }

  private def processPartition[T](iter: Iterator[Seq[T]]): Iterator[(T, DenseVector)] = {
    val localVocab = new java.util.concurrent.ConcurrentHashMap[T, Array[Double]]()

    for (seq <- iter) {
      val currentSeqSize = seq.length
      for (idx <- (0 until currentSeqSize)) {
        val currentWord = seq(idx)
        val context = ((idx - contextSize) to (idx + contextSize)).filter(i =>
          (i >= 0) && (i < currentSeqSize) && (i != idx))
        for (cIdx <- context) {
          val word = seq(cIdx)
          val weight = weightFunction(math.abs(cIdx - idx))
          val sign = 2.0 * signHash(word) - 1.0
          val embeddingIdx = valueHash(word)

          val currentEmbedding =
            localVocab.getOrDefault(currentWord, Array.fill(embeddingsDim)(0.0))
          currentEmbedding(embeddingIdx) += sign * weight
        }
      }
    }

    localVocab
      .entrySet()
      .asScala
      .map(entry => (entry.getKey(), new DenseVector(entry.getValue())))
      .iterator
  }
}
