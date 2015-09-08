package com.databricks.dfgraph

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Provides a method to run tests against a {@link SparkContext} variable that is correctly stopped
 * after each test.
 */
trait LocalSparkContext {
  /** Runs `f` on a new SparkContext and ensures that it is stopped afterwards. */
  def withSpark[T](f: SparkContext => T): T = {
    val conf = new SparkConf()
    val sc = new SparkContext("local", "test", conf)
    try {
      f(sc)
    } finally {
      sc.stop()
    }
  }
}

