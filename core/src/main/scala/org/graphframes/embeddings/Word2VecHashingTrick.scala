package org.graphframes.embeddings

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.udf
import org.apache.spark.unsafe.hash.Murmur3_x86_32.*
import org.apache.spark.unsafe.types.UTF8String
import org.graphframes.GraphFramesW2VException
import org.graphframes.rw.RandomWalkBase

/**
 * A Word2Vec implementation incorporating the hashing trick for efficient handling of large
 * vocabularies. This class applies multiple hashing functions to map input elements to a
 * fixed-size feature space before training a Word2Vec model. It is designed for scenarios with
 * high-dimensional or categorical data, such as graph embeddings from random walks.
 *
 * Key parameters include the number of hashing functions, hash space size, and standard Word2Vec
 * parameters like vector size, iterations, etc.
 */
class Word2VecHashingTrick extends Serializable {
  private var numHashingFuncs: Int = 5
  private var maxFeatures: Int = math.pow(2, 20).toInt
  private var seed: Long = new util.Random().nextLong()
  private var maxSentenceLength: Int = 1000
  private var minCount: Int = 5
  private var vectorSize: Int = 256
  private var numPartitions: Int = 1
  private var numIterations: Int = 1
  private var learningRate: Double = 0.025

  private var rwColName: String = RandomWalkBase.rwColName

  private var hashingFunctions: Array[(Any) => Int] = _

  /**
   * Sets the column name containing the sequences.
   *
   * @param value
   *   the column name to set
   * @return
   *   this instance
   */
  def setSequencesColumnName(value: String): this.type = {
    rwColName = value
    this
  }

  /**
   * Sets the learning rate for the Word2Vec model.
   *
   * @param value
   *   the learning rate to set
   * @return
   *   this instance
   */
  def setLearningRate(value: Double): this.type = {
    learningRate = value
    this
  }

  /**
   * Sets the number of hashing functions to use. More hashing functions improve embedding quality
   * by reducing hash collisions, but increase the training dataset size by multiplying rows by
   * the number of functions (e.g., 5 functions result in 5 times more rows).
   *
   * @param value
   *   the number of hashing functions to set
   * @return
   *   this instance
   */
  def setNumHashingFunctions(value: Int): this.type = {
    numHashingFuncs = value
    this
  }

  /**
   * Sets the maximum number of features in the hash space. Uses Murmur3 hash and simple modulo to
   * map original features to this fixed space. Powers of 2 are recommended for efficient modulo
   * operation.
   *
   * @param value
   *   the maximum number of features to set
   * @return
   *   this instance
   */
  def setMaxFeatures(value: Int): this.type = {
    maxFeatures = value
    this
  }

  /**
   * Sets the random seed for reproducibility.
   *
   * @param value
   *   the seed value to set
   * @return
   *   this instance
   */
  def setSeed(value: Long): this.type = {
    seed = value
    this
  }

  /**
   * Sets the maximum length of a sentence.
   *
   * @param value
   *   the maximum sentence length to set
   * @return
   *   this instance
   */
  def setMaxSentenceLength(value: Int): this.type = {
    maxSentenceLength = value
    this
  }

  /**
   * Sets the minimum number of times a word must appear to be included.
   *
   * @param value
   *   the minimum count to set
   * @return
   *   this instance
   */
  def setMinCount(value: Int): this.type = {
    minCount = value
    this
  }

  /**
   * Sets the size of the word vectors.
   *
   * @param value
   *   the vector size to set
   * @return
   *   this instance
   */
  def setVectorSize(value: Int): this.type = {
    vectorSize = value
    this
  }

  /**
   * Sets the number of partitions for parallel processing.
   *
   * @param value
   *   the number of partitions to set
   * @return
   *   this instance
   */
  def setNumPartitions(value: Int): this.type = {
    numPartitions = value
    this
  }

  /**
   * Sets the number of iterations for training.
   *
   * @param value
   *   the number of iterations to set
   * @return
   *   this instance
   */
  def setNumIterations(value: Int): this.type = {
    numIterations = value
    this
  }

  /**
   * This function body was copied from Apache Spark Utils
   */
  private def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  /**
   * This function body was copied from Apache Spark HashingTF
   */
  private def murmur3Hash(term: Any, seed: Int): Int = {
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
        throw new GraphFramesW2VException(
          "HashingTrick with murmur3 algorithm does not " +
            s"support type ${term.getClass.getCanonicalName} of input data.")
    }
  }

  private def doHashingTrick(sequences: DataFrame, embeddingCol: String): RDD[Seq[String]] = {
    val rng = new util.Random()
    rng.setSeed(42L)

    // generate collection of hashing functions;
    // each maps element from the original space
    // to the space L of the size "maxFeatures"
    hashingFunctions = (1 to numHashingFuncs)
      .map(_ => {
        val localSeed = rng.nextInt()
        (term: Any) => nonNegativeMod(murmur3Hash(term, localSeed), maxFeatures)
      })
      .toArray

    // apply hashing functions to row one by one;
    // it produces k-times longer dataset but without increase
    // the size of each individual row;
    //
    // TODO: it would be nice to test what is better,
    // wide-table way (duplicate elements inside rows)
    // or long-table way (duplicate rows by applying different hashes)
    val sequencesRDD: RDD[Seq[String]] =
      sequences
        .select(col(embeddingCol))
        .rdd
        .flatMap(seq => {
          val seqArray = seq.getSeq[Any](0)
          hashingFunctions.map(f => seqArray.map(el => f(el).toString()).toSeq)
        })

    sequencesRDD
  }

  /**
   * Fits a Word2Vec model to the input DataFrame by first applying the hashing trick to the
   * sequences in the specified column, then training a Word2Vec model on the hashed sequences.
   *
   * @param data
   *   DataFrame containing sequences (e.g., random walks) in the column set by
   *   setSequencesColumnName.
   * @return
   *   A Word2VecHashingTrickModel containing the trained Word2Vec model and hashing functions.
   */
  def fit(data: DataFrame): Word2VecHashingTrickModel = {
    val preparedSequences = doHashingTrick(data, rwColName)
    val word2vec = new Word2Vec()
      .setMaxSentenceLength(maxSentenceLength)
      .setMinCount(minCount)
      .setNumIterations(numIterations)
      .setVectorSize(vectorSize)
      .setNumPartitions(numPartitions)
      .setLearningRate(learningRate)
      .setSeed(seed)

    val w2vModel = word2vec.fit(preparedSequences)
    new Word2VecHashingTrickModel(w2vModel, hashingFunctions)
  }
}

/**
 * Model class for Word2VecHashingTrick. Encapsulates the trained Word2VecModel and the hashing
 * functions used during training. Provides methods to retrieve the model and to compute vector
 * representations for input identifiers by hashing them and averaging the corresponding word
 * vectors.
 */
class Word2VecHashingTrickModel private[graphframes] (
    private[graphframes] w2vModel: Word2VecModel,
    private[graphframes] hashingFunctions: Array[(Any) => Int])
    extends Serializable {

  /**
   * Retrieves the underlying Word2VecModel trained by the hashing trick approach.
   *
   * @return
   *   The trained Word2VecModel.
   */
  def getWord2VecModel: Word2VecModel = w2vModel

  /**
   * Computes vector representations for the given identifiers by applying the hashing functions,
   * looking up the hashed values in the Word2Vec model's vectors, and averaging them across the
   * multiple hash functions.
   *
   * @param ids
   *   DataFrame containing the identifiers to vectorize.
   * @param idCol
   *   Name of the column in `ids` containing the identifiers.
   * @return
   *   DataFrame with the original `idCol` and a "vector" column containing the averaged vectors.
   */
  def getVectors(ids: DataFrame, idCol: String): DataFrame = {
    val spark = ids.sparkSession
    val w2vVectors = w2vModel.getVectors.transform((_, vec) => Vectors.dense(vec.map(_.toDouble)))
    val hashedVetors = spark.createDataFrame(w2vVectors.toSeq).toDF("word", "vector")

    val hashingUDF = udf((id: Any) => hashingFunctions.map(f => f(id).toString()))

    val exploded = ids.select(col(idCol), explode(hashingUDF.apply(col(idCol)).alias("idHash")))

    val combined = exploded.join(hashedVetors, col("idHash") === col("word"), "left")

    combined.groupBy(col(idCol)).agg(Summarizer.mean(col("vector")).alias("vector"))
  }
}
