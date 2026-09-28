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

package org.graphframes.propertygraph.internal

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.*

/**
 * Whitelist-dispatched lowering of scalar function calls (`FunctionCall` AST nodes) to Spark SQL
 * built-in functions.
 *
 * Design: the registry is a strict 1:1 map from a (case-insensitive) function name to a single
 * `org.apache.spark.sql.functions` builtin. The whitelist ''is'' the scope boundary:
 *
 *   - Unknown names fail fast with `UnsupportedOperationException` naming the supported set --
 *     the same fail-fast philosophy as the resolver's unknown-label error.
 *   - Wrong arity fails fast with a clear message (`expects N argument(s), got M`).
 *   - The function name in the AST is lowercased at parse time; this layer normalizes once more
 *     for defense-in-depth.
 *
 * Two argument kinds are supported:
 *   - ordinary column arguments (`cols(i)`) lowered from the AST;
 *   - ''literal-typed'' arguments (`litStr`/`litInt`), for Spark builtins whose signature demands
 *     a `String`/`Int` literal rather than a `Column` (e.g. `get_json_object(col, path: String)`,
 *     `round(col, scale: Int)`). A non-literal passed where a literal is required fails fast with
 *     a clear message.
 *
 * UDFs, custom functions, etc. are not supported. While it is possible to extend the support, I
 * see no reason in it. At least until there is no explicit user-request.
 */
private[propertygraph] object FunctionRegistry {

  /**
   * Lower `rawName(astArgs)` to a Spark [[org.apache.spark.sql.Column]], validating name + arity.
   *
   * `astArgs` are the raw AST arguments (needed to extract literal-typed args); `cols` are the
   * same arguments already lowered to Spark columns (for ordinary column-arg builtins).
   */
  def lower(rawName: String, astArgs: Seq[Expression], cols: Seq[Column]): Column = {
    val name = rawName.toLowerCase
    def arity(n: Int): Unit =
      if (cols.length != n) {
        throw new UnsupportedOperationException(
          s"Function '$rawName' expects $n argument(s), got ${cols.length}")
      }
    def arityBetween(lo: Int, hi: Int): Unit =
      if (cols.length < lo || cols.length > hi) {
        throw new UnsupportedOperationException(
          s"Function '$rawName' expects between $lo and $hi argument(s), got ${cols.length}")
      }
    def arityAtLeast(n: Int): Unit =
      if (cols.length < n) {
        throw new UnsupportedOperationException(
          s"Function '$rawName' expects at least $n argument(s), got ${cols.length}")
      }

    name match {
      // *************** DATETIME BLOCK ********************************//
      case "year" => arity(1); year(cols(0))
      case "month" => arity(1); month(cols(0))
      case "day" | "dayofmonth" => arity(1); dayofmonth(cols(0))
      case "hour" => arity(1); hour(cols(0))
      case "minute" => arity(1); minute(cols(0))
      case "second" => arity(1); second(cols(0))
      case "quarter" => arity(1); quarter(cols(0))
      case "dayofweek" => arity(1); dayofweek(cols(0))
      case "dayofyear" => arity(1); dayofyear(cols(0))
      case "weekofyear" => arity(1); weekofyear(cols(0))
      case "date" | "to_date" => arity(1); to_date(cols(0)) // string/ts -> date
      case "to_timestamp" => arity(1); to_timestamp(cols(0))
      case "datediff" => arity(2); datediff(cols(0), cols(1)) // days, arg0 - arg1
      case "months_between" => arity(2); months_between(cols(0), cols(1))
      case "date_add" => arity(2); date_add(cols(0), cols(1))
      case "date_sub" => arity(2); date_sub(cols(0), cols(1))
      case "add_months" => arity(2); add_months(cols(0), cols(1))
      case "current_date" => arity(0); current_date()
      case "current_timestamp" => arity(0); current_timestamp()
      // **************************************************************//

      // *************** STRING BLOCK **********************************//
      case "lower" => arity(1); functions.lower(cols(0))
      case "upper" => arity(1); upper(cols(0))
      case "trim" => arity(1); trim(cols(0))
      case "ltrim" => arity(1); ltrim(cols(0))
      case "rtrim" => arity(1); rtrim(cols(0))
      case "length" => arity(1); length(cols(0))
      case "substr" | "substring" =>
        arity(3); cols(0).substr(cols(1), cols(2)) // Column.substr(Column, Column)
      case "concat" => arityAtLeast(1); concat(cols: _*)
      case "regexp_replace" => arity(3); regexp_replace(cols(0), cols(1), cols(2))
      case "regexp_extract" =>
        arity(3); regexp_extract(cols(0), litStr(astArgs(1)), litInt(astArgs(2)))
      case "rlike" | "regexp_like" => arity(2); cols(0).rlike(litStr(astArgs(1)))
      case "contains" => arity(2); cols(0).contains(cols(1))
      case "startswith" => arity(2); cols(0).startsWith(cols(1))
      case "endswith" => arity(2); cols(0).endsWith(cols(1))
      case "instr" => arity(2); instr(cols(0), litStr(astArgs(1)))
      case "split" => arity(2); split(cols(0), litStr(astArgs(1)))
      // **************************************************************//

      // *************** MATH BLOCK ************************************//
      case "abs" => arity(1); abs(cols(0))
      case "ceil" | "ceiling" => arity(1); ceil(cols(0))
      case "floor" => arity(1); floor(cols(0))
      case "round" =>
        arityBetween(1, 2)
        if (cols.length == 2) round(cols(0), litInt(astArgs(1))) else round(cols(0))
      case "sqrt" => arity(1); sqrt(cols(0))
      case "pmod" => arity(2); pmod(cols(0), cols(1))
      case "cbrt" => arity(1); cbrt(cols(0))
      case "pow" | "power" => arity(2); pow(cols(0), cols(1))
      case "exp" => arity(1); exp(cols(0))
      case "ln" | "log" => arity(1); log(cols(0)) // Spark `log` = natural log
      case "log10" => arity(1); log10(cols(0))
      case "log2" => arity(1); log2(cols(0))
      case "sign" | "signum" => arity(1); signum(cols(0))
      case "greatest" => arityAtLeast(2); greatest(cols: _*)
      case "least" => arityAtLeast(2); least(cols: _*)
      // **************************************************************//

      // *************** JSON BLOCK ************************************//
      case "get_json_object" => arity(2); get_json_object(cols(0), litStr(astArgs(1)))
      case "to_json" => arity(1); to_json(cols(0))
      // **************************************************************//

      // *************** XML XPATH BLOCK *******************************//
      // NB: Spark's `xpath_*` take (Column, Column), not (Column, String) -- so both args
      // are ordinary columns here, no lit-arg extraction needed.
      case "xpath_string" => arity(2); xpath_string(cols(0), cols(1))
      case "xpath_boolean" => arity(2); xpath_boolean(cols(0), cols(1))
      case "xpath_short" => arity(2); xpath_short(cols(0), cols(1))
      case "xpath_int" => arity(2); xpath_int(cols(0), cols(1))
      case "xpath_long" => arity(2); xpath_long(cols(0), cols(1))
      case "xpath_float" => arity(2); xpath_float(cols(0), cols(1))
      case "xpath_double" => arity(2); xpath_double(cols(0), cols(1))
      case "xpath" => arity(2); xpath(cols(0), cols(1))
      // **************************************************************//

      // *************** NULL / CONDITIONAL BLOCK **********************//
      case "coalesce" => arityAtLeast(1); coalesce(cols: _*)
      case "nullif" => arity(2); nullif(cols(0), cols(1))
      case "nvl" | "ifnull" => arity(2); ifnull(cols(0), cols(1))
      // **************************************************************//

      // ************************** HASH ******************************//
      case "hash" => arityAtLeast(1); hash(cols: _*) // signed 32-bit murmur3
      case "xxhash64" => arityAtLeast(1); xxhash64(cols: _*) // signed 64-bit
      case "md5" => arity(1); md5(cols(0))
      case "sha1" => arity(1); sha1(cols(0))
      case "crc32" => arity(1); crc32(cols(0))
      case "sha2" => arity(2); sha2(cols(0), litInt(astArgs(1)))
      // **************************************************************//

      case other =>
        throw new UnsupportedOperationException(
          s"Unsupported function '$other'. Supported: ${supported.mkString(", ")}")
    }
  }

  // -------------------------------------------------------------------------
  // Literal-typed argument extraction.
  //
  // Some Spark builtins take a `String`/`Int` literal rather than a `Column`
  // (e.g. `get_json_object(col, path: String)`, `round(col, scale: Int)`,
  // `regexp_extract(col, pattern: String, idx: Int)`). The lowered `Column`
  // cannot carry that, so we read from the AST `Literal` node instead. A
  // non-literal passed where a literal is required fails fast.
  // -------------------------------------------------------------------------

  private def litStr(e: Expression): String = e match {
    case Literal(s: String) => s
    case _ =>
      throw new UnsupportedOperationException(
        "argument must be a string literal (e.g. a JSON path or regex pattern)")
  }

  private def litInt(e: Expression): Int = e match {
    case Literal(v: Long) => v.toInt
    case Literal(v: Int) => v
    case _ =>
      throw new UnsupportedOperationException("argument must be an integer literal")
  }

  // -------------------------------------------------------------------------
  // Supported-function lists, one block per family. `supported` concatenates
  // them so the error message enumerates every accepted name (every alias is
  // listed, matching the convention that each alias is independently valid).
  // -------------------------------------------------------------------------

  private val supportedDTFunctions: Seq[String] = Seq(
    "year",
    "month",
    "day",
    "dayofmonth",
    "hour",
    "minute",
    "second",
    "quarter",
    "dayofweek",
    "dayofyear",
    "weekofyear",
    "date",
    "to_date",
    "to_timestamp",
    "datediff",
    "months_between",
    "date_add",
    "date_sub",
    "add_months",
    "current_date",
    "current_timestamp")

  private val supportedStringFunctions: Seq[String] = Seq(
    "lower",
    "upper",
    "trim",
    "ltrim",
    "rtrim",
    "length",
    "substr",
    "substring",
    "concat",
    "regexp_replace",
    "regexp_extract",
    "rlike",
    "regexp_like",
    "contains",
    "startswith",
    "endswith",
    "instr",
    "split")

  private val supportedMathFunctions: Seq[String] = Seq(
    "abs",
    "ceil",
    "ceiling",
    "floor",
    "round",
    "sqrt",
    "cbrt",
    "pow",
    "power",
    "pmod",
    "exp",
    "ln",
    "log",
    "log10",
    "log2",
    "sign",
    "signum",
    "greatest",
    "least")

  private val supportedJsonFunctions: Seq[String] = Seq("get_json_object", "to_json")

  private val supportedXmlFunctions: Seq[String] = Seq(
    "xpath_string",
    "xpath_boolean",
    "xpath_short",
    "xpath_int",
    "xpath_long",
    "xpath_float",
    "xpath_double",
    "xpath")

  private val supportedNullFunctions: Seq[String] = Seq("coalesce", "nullif", "nvl", "ifnull")

  private val supportedHashFunctions: Seq[String] =
    Seq("hash", "xxhash64", "md5", "sha1", "crc32", "sha2")

  private def supported: Seq[String] =
    supportedDTFunctions ++
      supportedStringFunctions ++
      supportedMathFunctions ++
      supportedJsonFunctions ++
      supportedXmlFunctions ++
      supportedNullFunctions ++
      supportedHashFunctions
}
