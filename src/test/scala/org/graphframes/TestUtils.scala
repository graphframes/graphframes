package org.graphframes

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, StructType}

import org.graphframes.GraphFrame._

object TestUtils {

  private[this] val majorMinorRegex = """^(\d+)\.(\d+)(\..*)?$""".r

  /** Extract major/minor version integer pairs from a version string */
  def majorMinorVersion(sparkVersion: String): (Int, Int) = {
    majorMinorRegex.findFirstMatchIn(sparkVersion) match {
      case Some(m) =>
        (m.group(1).toInt, m.group(2).toInt)
      case None =>
        throw new IllegalArgumentException(s"Spark tried to parse '$sparkVersion' as a Spark" +
          s" version string, but it could not find the major and minor version numbers.")
    }
  }

  /**
   * Check whether the given schema contains a column of the required data type.
   *
   * @param colName  column name
   * @param dataType  required column data type
   */
  def checkColumnType(
      schema: StructType,
      colName: String,
      dataType: DataType,
      msg: String = ""): Unit = {
    val actualDataType = schema(colName).dataType
    val message = if (msg != null && msg.trim.length > 0) " " + msg else ""
    require(actualDataType.equals(dataType),
      s"Column $colName must be of type $dataType but was actually $actualDataType.$message")
  }

  /** Confirm ID, SRC, DST columns are present */
  def testSchemaInvariant(g: GraphFrame): Unit = {
    val vCols = g.vertices.columns
    val eCols = g.edges.columns
    assert(vCols.contains(ID))
    assert(eCols.contains(SRC))
    assert(eCols.contains(DST))
  }

  /**
   * Test validity of both GraphFrames.
   * Also ensure that the GraphFrames match:
   *  - vertex column schema match
   *  - `before` columns are a subset of the `after` columns, and schema match
   */
  def testSchemaInvariants(before: GraphFrame, after: GraphFrame): Unit = {
    testSchemaInvariant(before)
    testSchemaInvariant(after)
    // The IDs, source and destination columns should be of the same type
    // with the same metadata.
    for (colName <- Seq(ID)) {
      val b = before.vertices.schema(colName)
      val a = after.vertices.schema(colName)
      // TODO(tjh) check nullability and metadata
      assert(a.dataType == b.dataType, (a, b))
    }
    for (colName <- Seq(SRC, DST)) {
      val b = before.edges.schema(colName)
      val a = after.edges.schema(colName)
      // TODO(tjh) check nullability and metadata
      assert(a.dataType == b.dataType, (a, b))
    }
    // All the columns before should be found after (with some extra columns,
    // potentially).
    val afterVNames = before.vertices.schema.fields.map(_.name)
    for (f <- before.vertices.schema.iterator) {
      if (!afterVNames.contains(f.name)) {
        throw new Exception(s"vertex error: ${f.name} should be in ${afterVNames.mkString(", ")}")
      }
      assert(before.vertices.schema(f.name) == after.vertices.schema(f.name),
        s"${before.vertices.schema} != ${after.vertices.schema}")
    }

    for (f <- before.edges.schema.iterator) {
      val a = before.edges.schema(f.name)
      val b = after.edges.schema(f.name)
      assert(a.dataType == b.dataType,
        s"${before.edges.schema} not a subset of ${after.edges.schema}")
    }
  }

  /**
   * Test validity of both GraphFrames.
   * Also ensure that the GraphFrames match:
   *  - vertex column schema match
   *  - `before` columns are a subset of the `after` columns, and schema match
   */
  def testSchemaInvariants(before: GraphFrame, afterVertices: DataFrame): Unit = {
    testSchemaInvariants(before, GraphFrame(afterVertices, before.edges))
  }

}
