package org.graphframes.rw

import org.apache.spark.sql.functions.array_size
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StringType
import org.graphframes.GraphFrameTestSparkContext
import org.graphframes.SparkFunSuite
import org.graphframes.examples.Graphs

class RandomWalkWithRestartSuite extends SparkFunSuite with GraphFrameTestSparkContext {
  test("test RW base") {
    val g = Graphs.friends

    val rwRunner = new RandomWalkWithRestart()
      .onGraph(g)
      .setRestartProbability(0.2)
      .setGlobalSeed(42L)
      .setBatchSize(5)
      .setNumBatches(5)
      .setNumWalksPerNode(10)
      .setTemporaryPrefix("/tmp")

    val runId = rwRunner.getRunId()
    try {
      val walks = rwRunner.run()
      
      assert(walks.schema.fields.length === 2)
      // friends has string as ID type
      assert(walks.schema(RandomWalkBase.rwColName).dataType === ArrayType(StringType))

      // num rows should be:
      // - 10 walks for each vertex that has edge
      // - vertex "g" is isolated
      // - total vertices 7
      // - total walks (7 - 1) * 10 = 60
      assert(walks.count() === 60)

      // each walk should have length numBatches * batchSize = 25
      assert(walks.filter(array_size(col(RandomWalkBase.rwColName)) =!= lit(25)).count() === 0)

      // all walk IDs are unique
      assert(walks.select(col(RandomWalkBase.walkIdCol)).distinct().count() === 60)
    } finally {
      // Clean up temporary files after the test
      rwRunner.cleanUp(runId)
    }
  }
}
