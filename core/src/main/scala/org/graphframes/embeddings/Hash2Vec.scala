package org.graphframes.embeddings

import dev.ludovic.netlib.blas.BLAS
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.hash
import org.apache.spark.sql.functions.pmod
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.transform
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.hash.Murmur3_x86_32.*
import org.graphframes.GraphFramesUnsupportedVertexTypeException
import org.graphframes.rw.RandomWalkBase

import scala.reflect.ClassTag
import org.apache.spark.sql.Column
import scala.util.hashing.MurmurHash3

/**
 * Implementation of Hash2Vec, an efficient word embedding technique using feature hashing. Based
 * on: Argerich, Luis, Joaquín Torré Zaffaroni, and Matías J. Cano. "Hash2vec, feature hashing for
 * word embeddings." arXiv preprint arXiv:1608.08940 (2016).
 *
 * Produces embeddings for elements in sequences using a hash-based approach to avoid storing a
 * vocabulary. Uses MurmurHash3 for hashing elements to embedding indices and signs.
 *
 * Output DataFrame has columns "id" (element identifier, same type as sequence elements) and
 * "vector" (dense vector of doubles, summed across all occurrences).
 *
 * Tradeoffs: Higher numPartitions reduces local state and memory per partition but increases
 * aggregation and merging overhead across partitions. Larger embeddingsDim provides richer
 * representations but consumes more memory. Seeds control hashing for reproducibility.
 */
class Hash2Vec extends Serializable {
  private def decayGaussian(d: Int, sigma: Double): Double = {
    math.exp(-(d * d) / (sigma * sigma))
  }
  private val possibleDecayFunctions: Seq[String] = Seq("gaussian", "constant")

  private var contextSize: Int = 5
  private var numPartitions: Int = 5
  private var embeddingsDim: Int = 512
  private var sequenceCol: String = RandomWalkBase.rwColName
  private var decayFunction: String = "gaussian"
  private var gaussianSigma: Double = 1.0
  private var hashingSeed: Int = 42
  private var signHashingSeed: Int = 18
  private var doL2Norm: Boolean = false
  private var safeL2NormAsChannel: Boolean = true

  def setDoNormalization(doNorm: Boolean, safeNorm: Boolean): this.type = {
    doL2Norm = doNorm
    safeL2NormAsChannel = safeNorm
    this
  }

  def setDoNormalization(value: Boolean): this.type = {
    setDoNormalization(value, true)
    this
  }

  /**
   * Sets the context window size around each element to consider during training. Larger values
   * incorporate more distant elements but increase computation time. Default: 5.
   */
  def setContextSize(value: Int): this.type = {
    contextSize = value
    this
  }

  /**
   * Sets the number of partitions for RDDs to parallelize computation. More partitions distribute
   * workload and reduce memory per partition but complicate merging across partitions. Default:
   * 5.
   */
  def setNumPartitions(value: Int): this.type = {
    numPartitions = value
    this
  }

  /**
   * Sets the dimensionality of the dense embedding vectors. Larger dimensions allow richer
   * representations but require more memory. Corresponds to the hash table size. Default: 256.
   */
  def setEmbeddingsDim(value: Int): this.type = {
    embeddingsDim = value
    this
  }

  /**
   * Sets the column name containing sequences of elements (as arrays). Default: "random_walk".
   */
  def setSequenceCol(value: String): this.type = {
    sequenceCol = value
    this
  }

  /**
   * Sets the decay function used to weight context elements by distance. Supported values:
   * "gaussian", "constant". Default: "gaussian".
   */
  def setDecayFunction(value: String): this.type = {
    val sep = ", "
    require(
      possibleDecayFunctions.contains(value),
      s"supported functions: ${possibleDecayFunctions.mkString(sep)}")
    decayFunction = value
    this
  }

  /**
   * Sets the sigma parameter for Gaussian decay weighting. Smaller values decay weights faster
   * with distance. Default: 1.0.
   */
  def setGaussianSigma(value: Double): this.type = {
    gaussianSigma = value
    this
  }

  /**
   * Sets the seed for hashing elements to embedding indices. Used for reproducibility of
   * embeddings. Default: 42.
   */
  def setHashingSeed(value: Int): this.type = {
    hashingSeed = value
    this
  }

  /**
   * Sets the seed for hashing elements to determine the sign of contributions. Used for
   * reproducibility of embeddings. Default: 18.
   */
  def setSignHashSeed(value: Int): this.type = {
    signHashingSeed = value
    this
  }

  private def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  private var weightFunction: (Int) => Double = _

  private def seededStringHashFunc(s: String, seed: Int, dim: Int): Int = {
    var h = seed
    var i = 0
    val len = s.length
    while (i < len) {
      h = MurmurHash3.mix(h, s.charAt(i).toInt)
      i += 1
    }
    nonNegativeMod(MurmurHash3.finalizeHash(h, len), dim)
  }

  private def seededLongHashFunc(l: Long, seed: Int, dim: Int): Int =
    nonNegativeMod(hashLong(l, seed), dim)

  private def normalize(vector: linalg.Vector, addChannel: Boolean): linalg.Vector = {
    val blas = BLAS.getInstance()
    val arr = vector.toArray
    val norm = blas.dnrm2(arr.size, arr, 0, 1)
    blas.dscal(arr.size, 1 / (norm + 1e-6), arr, 1)
    if (addChannel) {
      val scaledL2 = math.log(norm + 1)
      val newChannel = scaledL2 / math.sqrt(vector.size.toDouble)
      new linalg.DenseVector(arr :+ newChannel)
    } else {
      new linalg.DenseVector(arr)
    }
  }

  /**
   * Runs the Hash2Vec algorithm on the input DataFrame containing sequences. The specified
   * sequenceCol must contain arrays of elements (string or numeric). Produces a DataFrame with
   * "id" (element ID, same type as elements) and "vector" (embedding vector, VectorType).
   * Embeddings are summed across all partitions and occurrences.
   */
  def run(rawData: DataFrame): DataFrame = {
    val spark = rawData.sparkSession
    require(
      rawData.schema(sequenceCol).dataType.isInstanceOf[ArrayType],
      "sequence should be array")
    val elDataType = rawData.schema(sequenceCol).dataType.asInstanceOf[ArrayType].elementType

    val data =
      if (elDataType.isInstanceOf[ByteType] || elDataType.isInstanceOf[ShortType] || elDataType
          .isInstanceOf[IntegerType]) {
        rawData.withColumn(
          sequenceCol,
          transform(col(sequenceCol), (f: Column) => f.cast(LongType)))
      } else {
        rawData
      }

    weightFunction = decayFunction match {
      case "gaussian" => (d: Int) => decayGaussian(d, gaussianSigma)
      case "constant" => (_: Int) => 1.0
      case _ => throw new RuntimeException(s"unsupported decay functions $decayFunction")
    }

    val (rowRDD, schema) = elDataType match {
      case _: StringType =>
        (
          runTyped[String](data).map(f => Row(f._1, Vectors.dense(f._2))),
          StructType(Seq(StructField("id", StringType), StructField("vector", VectorType))))
      case _: LongType =>
        (
          runTyped[Long](data).map(f => Row(f._1, Vectors.dense(f._2))),
          StructType(Seq(StructField("id", LongType), StructField("vector", VectorType))))
      case _ =>
        throw new GraphFramesUnsupportedVertexTypeException(
          s"Hash2vec supports only string or numeric types of elements but gor ${elDataType.toString()}")
    }

    val embeddings = spark
      .createDataFrame(rowRDD, schema)
      .groupBy("id")
      .agg(Summarizer.sum(col("vector")).alias("vector"))

    val normalizer = (x: linalg.Vector) => normalize(x, safeL2NormAsChannel)

    if (doL2Norm) {
      embeddings.withColumn("vector", udf(normalizer).apply(col("vector")))
    } else {
      embeddings
    }
  }

  private def runTyped[T: ClassTag](data: DataFrame): RDD[(T, Array[Double])] = {
    // we should put sequences starts from the same vertex
    // to the same partition when possible
    data
      .withColumn("hash_id", pmod(hash(col(sequenceCol).getItem(0)), lit(numPartitions)))
      .repartition(numPartitions, col("hash_id"))
      .sortWithinPartitions(col("hash_id"))
      .select(col(sequenceCol))
      .rdd
      .map(_.getSeq[T](0))
      .mapPartitions { iter =>
        val elemType = implicitly[ClassTag[T]].runtimeClass
        elemType match {
          case clazz if clazz == classOf[String] =>
            processStringPartition(iter.asInstanceOf[Iterator[Seq[String]]])
              .asInstanceOf[Iterator[(T, Array[Double])]]
          case clazz if clazz == classOf[Long] =>
            processLongPartition(iter.asInstanceOf[Iterator[Seq[Long]]])
              .asInstanceOf[Iterator[(T, Array[Double])]]
          case _ =>
            throw new GraphFramesUnsupportedVertexTypeException(
              s"Hash2vec does not support type ${elemType.getCanonicalName}")
        }
      }
  }

  // Specialized partition processing for String
  private def processStringPartition(
      iter: Iterator[Seq[String]]): Iterator[(String, Array[Double])] = {

    val localHashSeed = hashingSeed
    val localSignHashSeed = signHashingSeed
    val localEmbeddingsDim = embeddingsDim

    val localVocab = new collection.mutable.HashMap[String, Array[Double]]()

    val weightCache = new Array[Double](contextSize + 1)
    for (d <- 1 to contextSize) weightCache(d) = weightFunction(d)
    val signs = Array[Double](-1.0, 1.0)

    while (iter.hasNext) {
      val seq = iter.next()
      val currentSeqSize = seq.length
      var idx = 0

      while (idx < currentSeqSize) {
        val currentWord = seq(idx)
        var embedding: Array[Double] = localVocab.getOrElse(currentWord, null)
        if (embedding == null) {
          embedding = new Array[Double](localEmbeddingsDim)
          localVocab.put(currentWord, embedding)
        }
        val start = math.max(0, idx - contextSize)
        val end = math.min(currentSeqSize - 1, idx + contextSize)
        var cIdx = start

        while (cIdx <= end) {
          if (cIdx != idx) {
            val word = seq(cIdx)
            val embeddingIdx = seededStringHashFunc(word, localHashSeed, localEmbeddingsDim)
            val sign = signs(seededStringHashFunc(word, localSignHashSeed, 2))
            val weight = weightCache(math.abs(cIdx - idx))
            embedding(embeddingIdx) += sign * weight
          }
          cIdx += 1
        }
        idx += 1
      }
    }

    localVocab.iterator
  }

  // Specialized partition processing for Long
  private def processLongPartition(iter: Iterator[Seq[Long]]): Iterator[(Long, Array[Double])] = {

    val localHashSeed = hashingSeed
    val localSignHashSeed = signHashingSeed
    val localEmbeddingsDim = embeddingsDim

    val vocabIndex = new collection.mutable.HashMap[Long, Int]()
    vocabIndex.sizeHint(100000)
    val matrix = Hash2Vec.PagedMatrixDouble(localEmbeddingsDim)

    val weightCache = new Array[Double](contextSize + 1)
    for (d <- 1 to contextSize) weightCache(d) = weightFunction(d)
    val signs = Array[Double](-1.0, 1.0)

    while (iter.hasNext) {
      val seq = iter.next()
      val currentSeqSize = seq.length
      var idx = 0

      while (idx < currentSeqSize) {
        val currentWord = seq(idx)

        var vectorId = vocabIndex.getOrElse(currentWord, -1)

        if (vectorId == -1) {
          vectorId = matrix.allocateVector()
          vocabIndex.put(currentWord, vectorId)
        }

        val start = math.max(0, idx - contextSize)
        val end = math.min(currentSeqSize - 1, idx + contextSize)
        var cIdx = start

        while (cIdx <= end) {
          if (cIdx != idx) {
            val word = seq(cIdx)
            val embeddingIdx = seededLongHashFunc(word, localHashSeed, localEmbeddingsDim)
            val sign = signs(seededLongHashFunc(word, localSignHashSeed, 2))
            val weight = weightCache(math.abs(cIdx - idx))

            matrix.add(vectorId, embeddingIdx, sign * weight)
          }
          cIdx += 1
        }
        idx += 1
      }
    }

    vocabIndex.iterator.map { case (word, vectorId) =>
      (word, matrix.getVector(vectorId))
    }
  }
}

object Hash2Vec {
  // ==================================================================
  // HELPER: Paged Matrix for Double (Unlimited Memory)
  // ==================================================================
  private[graphframes] case class PagedMatrixDouble(val dim: Int) {
    private final val PAGE_BITS = 16
    private final val PAGE_SIZE = 1 << PAGE_BITS // 65536 -> 2^16
    private final val PAGE_MASK = PAGE_SIZE - 1 // 0xFFFF

    private val pages = new collection.mutable.ArrayBuffer[Array[Double]]()
    private var vectorCount = 0

    addPage()

    private def addPage(): Unit = {
      val size = PAGE_SIZE.toLong * dim
      if (size > Int.MaxValue) {
        throw new RuntimeException(s"Dimension $dim is too large for current Page Size.")
      }
      pages += new Array[Double](size.toInt)
    }

    def allocateVector(): Int = {
      val id = vectorCount
      val localIdx = id & PAGE_MASK // ~id % 65536

      if (localIdx == 0 && id > 0) {
        addPage()
      }

      vectorCount += 1
      id
    }

    @inline
    def add(vectorId: Int, offset: Int, value: Double): Unit = {
      // vectorId / 65536
      val pageIdx = vectorId >>> PAGE_BITS
      // vectorId % 65536
      val localRow = vectorId & PAGE_MASK

      val idx = (localRow * dim) + offset
      pages(pageIdx)(idx) += value
    }

    def getVector(vectorId: Int): Array[Double] = {
      val pageIdx = vectorId >>> PAGE_BITS
      val localRow = vectorId & PAGE_MASK
      val page = pages(pageIdx)

      val res = new Array[Double](dim)
      val startPos = localRow * dim
      System.arraycopy(page, startPos, res, 0, dim)
      res
    }
  }
}
