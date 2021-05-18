/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.graphframes.lib

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.types.DataTypes

import org.graphframes.{GraphFrame, GraphFrameTestSparkContext, SparkFunSuite, TestUtils}
import org.graphframes.examples.Graphs


class SVDPlusPlusSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  test("Test SVD++ with mean square error on training set") {
    val svdppErr = 8.0
    val g = Graphs.ALSSyntheticData()

    val v2 = g.svdPlusPlus.maxIter(2).run()
    TestUtils.testSchemaInvariants(g, v2)
    Seq(SVDPlusPlus.COLUMN1, SVDPlusPlus.COLUMN2).foreach { case c =>
      TestUtils.checkColumnType(v2.schema, c,
        DataTypes.createArrayType(DataTypes.DoubleType, false))
    }
    Seq(SVDPlusPlus.COLUMN3, SVDPlusPlus.COLUMN4).foreach { case c =>
      TestUtils.checkColumnType(v2.schema, c, DataTypes.DoubleType)
    }
    val err = v2.select(GraphFrame.ID, SVDPlusPlus.COLUMN4).rdd.map {
      case Row(vid: Long, vd: Double) =>
        if (vid % 2 == 1) vd else 0.0
    }.reduce(_ + _) / g.edges.count()
    assert(err <= svdppErr)
  }

  test("Invalid SVDPlusPlus parameters"){
    val g =  Graphs.empty[Int]

    withClue("SVDPlusPlus rank should be greater than 0"){
      intercept[IllegalArgumentException]{
        g.svdPlusPlus.rank(0).run()
      }
    }

    withClue("SVDPlusPlus maxIter should be greater than 0"){
      intercept[IllegalArgumentException]{
        g.svdPlusPlus.maxIter(0).run()
      }
    }

    withClue("SVDPlusPlus minValue should not be NaN"){
      intercept[IllegalArgumentException]{
        g.svdPlusPlus.minValue(Double.NaN).run()
      }
    }

    withClue("SVDPlusPlus maxValue should not be NaN"){
      intercept[IllegalArgumentException]{
        g.svdPlusPlus.maxValue(Double.NaN).run()
      }
    }

    withClue("SVDPlusPlus gamma1 should not be NaN"){
      intercept[IllegalArgumentException]{
        g.svdPlusPlus.gamma1(Double.NaN).run()
      }
    }

    withClue("SVDPlusPlus gamma2 should not be NaN"){
      intercept[IllegalArgumentException]{
        g.svdPlusPlus.gamma2(Double.NaN).run()
      }
    }

    withClue("SVDPlusPlus gamma6 should not be NaN"){
      intercept[IllegalArgumentException]{
        g.svdPlusPlus.gamma6(Double.NaN).run()
      }
    }

    withClue("SVDPlusPlus gamma7 should not be NaN"){
      intercept[IllegalArgumentException]{
        g.svdPlusPlus.gamma7(Double.NaN).run()
      }
    }

    withClue("SVDPlusPlus minValue should not equal maxValue"){
      intercept[AnalysisException]{
        g.svdPlusPlus.minValue(1.0).maxValue(1.0).run()
      }
    }
  }
}
