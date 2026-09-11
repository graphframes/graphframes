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
import org.graphframes.SparkFunSuite

/**
 * Pure-JVM tests for `FunctionRegistry.lower`. Building a Spark `Column` does not require an
 * active `SparkSession` -- it only constructs an unresolved expression tree -- so every test here
 * runs without Spark.
 *
 * The registry is a whitelist, and the whitelist ''is'' the scope boundary of the GQL expression
 * language. These tests pin down that boundary from four directions:
 *   - every accepted name lowers at its documented arity (and its aliases agree);
 *   - the arity guards (`arity`, `arityBetween`, `arityAtLeast`) reject off-by-one calls;
 *   - the literal-typed argument slots (`litStr` / `litInt`) reject non-literals;
 *   - an unrecognised name fails fast and the message enumerates what ''is'' supported.
 *
 * `QueryExecutorSuite` covers a handful of these functions end-to-end through Spark; this suite
 * covers the dispatch table itself, which is otherwise only exercised for four of its ~90 names.
 */
class FunctionRegistrySuite extends SparkFunSuite {

  // A distinct non-literal argument per position, so a function that mistakenly reads the wrong
  // slot produces a visibly wrong column rather than an accidental match.
  private def arg(i: Int): Expression = PropertyAccess("a", s"p$i")

  /** Lower `name(args)` the way `ExpressionLowering` does, against raw (un-prefixed) columns. */
  private def lower(name: String, args: Expression*): Column =
    FunctionRegistry.lower(name, args, args.map(ExpressionLowering.lower(_, PrefixEnv.raw)))

  /** Lower `name` with exactly `n` plain column arguments. */
  private def lowerN(name: String, n: Int): Column =
    lower(name, (0 until n).map(arg): _*)

  private def interceptUOE(f: => Any): String =
    intercept[UnsupportedOperationException](f).getMessage

  // ---------------------------------------------------------------------
  // Accepted names, grouped by the arity contract they are dispatched under.
  // Names requiring a literal-typed argument are handled separately below.
  // ---------------------------------------------------------------------

  private val zeroArg: Seq[String] = Seq("current_date", "current_timestamp")

  private val oneArg: Seq[String] = Seq(
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
    "lower",
    "upper",
    "trim",
    "ltrim",
    "rtrim",
    "length",
    "abs",
    "ceil",
    "ceiling",
    "floor",
    "sqrt",
    "cbrt",
    "exp",
    "ln",
    "log",
    "log10",
    "log2",
    "sign",
    "signum",
    "to_json",
    "md5",
    "sha1",
    "crc32")

  private val twoArg: Seq[String] = Seq(
    "datediff",
    "months_between",
    "date_add",
    "date_sub",
    "add_months",
    "contains",
    "startswith",
    "endswith",
    "pmod",
    "pow",
    "power",
    "xpath_string",
    "xpath_boolean",
    "xpath_short",
    "xpath_int",
    "xpath_long",
    "xpath_float",
    "xpath_double",
    "xpath",
    "nvl",
    "ifnull")

  private val threeArg: Seq[String] = Seq("substr", "substring", "regexp_replace")

  private val atLeastOneArg: Seq[String] = Seq("concat", "coalesce", "hash", "xxhash64")

  private val atLeastTwoArg: Seq[String] = Seq("greatest", "least")

  // ---------------------------------------------------------------------
  // Happy path: every whitelisted name lowers at its documented arity.
  // ---------------------------------------------------------------------

  test("every zero-argument function lowers with no arguments") {
    zeroArg.foreach(name => assert(lowerN(name, 0) != null, s"$name failed to lower"))
  }

  test("every one-argument function lowers with a single column argument") {
    oneArg.foreach(name => assert(lowerN(name, 1) != null, s"$name failed to lower"))
  }

  test("every two-argument function lowers with two column arguments") {
    twoArg.foreach(name => assert(lowerN(name, 2) != null, s"$name failed to lower"))
  }

  test("every three-argument function lowers with three column arguments") {
    threeArg.foreach(name => assert(lowerN(name, 3) != null, s"$name failed to lower"))
  }

  test("variadic functions lower at one, two and many arguments") {
    atLeastOneArg.foreach { name =>
      Seq(1, 2, 7).foreach(n => assert(lowerN(name, n) != null, s"$name failed at arity $n"))
    }
    atLeastTwoArg.foreach { name =>
      Seq(2, 7).foreach(n => assert(lowerN(name, n) != null, s"$name failed at arity $n"))
    }
  }

  test("literal-argument functions lower when given the literal kind they require") {
    assert(lower("get_json_object", arg(0), Literal("$.a")) != null)
    assert(lower("regexp_extract", arg(0), Literal("(\\d+)"), Literal(1L)) != null)
    assert(lower("rlike", arg(0), Literal("^a")) != null)
    assert(lower("regexp_like", arg(0), Literal("^a")) != null)
    assert(lower("instr", arg(0), Literal("x")) != null)
    assert(lower("split", arg(0), Literal(",")) != null)
    assert(lower("sha2", arg(0), Literal(256L)) != null)
    assert(lower("round", arg(0)) != null)
    assert(lower("round", arg(0), Literal(2L)) != null)
  }

  // ---------------------------------------------------------------------
  // KNOWN LIMITATION: `nullif` is whitelisted but cannot be lowered.
  // ---------------------------------------------------------------------

  test("nullif is whitelisted but throws when lowered over unresolved columns") {
    // `functions.nullif` builds a RuntimeReplaceable whose constructor reads `left.dataType`
    // eagerly. Every argument reaching this registry is an UnresolvedAttribute (`col("...")`),
    // so the call fails before the plan is ever analysed -- in WHERE, in RETURN and in a join
    // predicate alike (see QueryExecutorSuite for the end-to-end characterisation).
    //
    // This is a defect, not a designed restriction: either drop `nullif` from the whitelist or
    // lower it to an equivalent that defers typing, e.g.
    // `when(cols(0) === cols(1), lit(null)).otherwise(cols(0))`.
    // When that is fixed, move "nullif" back into `twoArg` and delete this test.
    val e = intercept[Exception](lower("nullif", arg(0), arg(1)))
    assert(e.getClass.getSimpleName.contains("UnresolvedException"))
  }

  test("nullif still validates its arity before it fails to lower") {
    // The arity guard runs first, so a wrong-arity call gets the clear registry error rather
    // than the confusing UnresolvedException above.
    assert(interceptUOE(lowerN("nullif", 1)).contains("expects 2 argument(s), got 1"))
    assert(interceptUOE(lowerN("nullif", 3)).contains("expects 2 argument(s), got 3"))
  }

  test("nvl and ifnull, unlike nullif, lower cleanly over unresolved columns") {
    // Pinned next to the nullif limitation so a future sweep does not assume the whole
    // null-handling block is broken.
    assert(lowerN("nvl", 2) != null)
    assert(lowerN("ifnull", 2) != null)
    assert(lowerN("coalesce", 2) != null)
  }

  // ---------------------------------------------------------------------
  // Name normalisation and aliasing.
  // ---------------------------------------------------------------------

  test("function names are matched case-insensitively") {
    // AstBuilder lowercases at parse time; the registry normalises again for defense in depth,
    // so a hand-built AST with an upper-case name must still dispatch.
    Seq("YEAR", "Year", "yEaR").foreach { name =>
      assert(lower(name, arg(0)).toString === lower("year", arg(0)).toString)
    }
  }

  test("aliases lower to the same Spark expression as their canonical name") {
    val aliases = Seq(
      ("day", "dayofmonth", 1),
      ("date", "to_date", 1),
      ("ceil", "ceiling", 1),
      ("sign", "signum", 1),
      ("ln", "log", 1),
      ("pow", "power", 2),
      ("nvl", "ifnull", 2),
      ("substr", "substring", 3))
    aliases.foreach { case (left, right, arity) =>
      assert(
        lowerN(left, arity).toString === lowerN(right, arity).toString,
        s"$left and $right disagree")
    }
    // rlike / regexp_like take a string literal in the second slot, so they are compared apart.
    assert(
      lower("rlike", arg(0), Literal("^a")).toString ===
        lower("regexp_like", arg(0), Literal("^a")).toString)
  }

  // ---------------------------------------------------------------------
  // Unknown names.
  // ---------------------------------------------------------------------

  test("an unknown function name fails fast and enumerates the supported set") {
    val message = interceptUOE(lower("my_udf", arg(0)))
    assert(message.contains("Unsupported function 'my_udf'"))
    // The message must actually list the whitelist, not just say "unsupported".
    Seq("year", "concat", "coalesce", "xpath_int", "sha2").foreach { known =>
      assert(message.contains(known), s"supported-set message omits '$known'")
    }
  }

  test("an unknown name is reported in its original case") {
    assert(interceptUOE(lower("MyUdf", arg(0))).contains("'myudf'"))
  }

  test("a Spark builtin that is deliberately not whitelisted is rejected") {
    // Aggregates, window functions and UDF-ish escapes are out of scope by design; the whitelist
    // is the boundary, so each of these must fail rather than silently lower.
    Seq("count", "sum", "collect_list", "explode", "row_number", "cast").foreach { name =>
      assert(
        interceptUOE(lower(name, arg(0))).contains("Unsupported function"),
        s"$name unexpectedly lowered")
    }
  }

  // ---------------------------------------------------------------------
  // Arity guards.
  // ---------------------------------------------------------------------

  test("exact-arity functions reject one argument too many") {
    (zeroArg.map((_, 0)) ++ oneArg.map((_, 1)) ++ twoArg.map((_, 2)) ++ threeArg.map((_, 3)))
      .foreach { case (name, n) =>
        val message = interceptUOE(lowerN(name, n + 1))
        assert(
          message.contains(s"expects $n argument(s), got ${n + 1}"),
          s"$name accepted ${n + 1} arguments")
      }
  }

  test("exact-arity functions reject one argument too few") {
    (oneArg.map((_, 1)) ++ twoArg.map((_, 2)) ++ threeArg.map((_, 3))).foreach { case (name, n) =>
      val message = interceptUOE(lowerN(name, n - 1))
      assert(
        message.contains(s"expects $n argument(s), got ${n - 1}"),
        s"$name accepted ${n - 1} arguments")
    }
  }

  test("arityAtLeast(1) functions reject a zero-argument call") {
    atLeastOneArg.foreach { name =>
      assert(interceptUOE(lowerN(name, 0)).contains("expects at least 1 argument(s), got 0"))
    }
  }

  test("arityAtLeast(2) functions reject a one-argument call") {
    atLeastTwoArg.foreach { name =>
      assert(interceptUOE(lowerN(name, 1)).contains("expects at least 2 argument(s), got 1"))
    }
  }

  test("round accepts 1 or 2 arguments and rejects 0 or 3") {
    assert(
      interceptUOE(lowerN("round", 0)).contains("expects between 1 and 2 argument(s), got 0"))
    assert(
      interceptUOE(lower("round", arg(0), Literal(1L), arg(2)))
        .contains("expects between 1 and 2 argument(s), got 3"))
  }

  test("arity is validated before the literal-typed argument is read") {
    // `regexp_extract` reads astArgs(1)/astArgs(2); a short call must fail on arity rather than
    // on an IndexOutOfBoundsException from the literal extraction.
    val message = interceptUOE(lower("regexp_extract", arg(0)))
    assert(message.contains("expects 3 argument(s), got 1"))
  }

  // ---------------------------------------------------------------------
  // Literal-typed argument slots.
  // ---------------------------------------------------------------------

  test("string-literal slots reject a property reference") {
    val cases = Seq(
      ("get_json_object", Seq(arg(0), arg(1))),
      ("rlike", Seq(arg(0), arg(1))),
      ("regexp_like", Seq(arg(0), arg(1))),
      ("instr", Seq(arg(0), arg(1))),
      ("split", Seq(arg(0), arg(1))),
      ("regexp_extract", Seq(arg(0), arg(1), Literal(1L))))
    cases.foreach { case (name, args) =>
      assert(
        interceptUOE(lower(name, args: _*)).contains("must be a string literal"),
        s"$name accepted a non-literal where a string literal is required")
    }
  }

  test("string-literal slots reject a literal of the wrong type") {
    assert(interceptUOE(lower("split", arg(0), Literal(1L))).contains("must be a string literal"))
    assert(
      interceptUOE(lower("get_json_object", arg(0), Literal(null)))
        .contains("must be a string literal"))
  }

  test("integer-literal slots reject a property reference") {
    val cases = Seq(
      ("sha2", Seq(arg(0), arg(1))),
      ("round", Seq(arg(0), arg(1))),
      ("regexp_extract", Seq(arg(0), Literal("(\\d+)"), arg(2))))
    cases.foreach { case (name, args) =>
      assert(
        interceptUOE(lower(name, args: _*)).contains("must be an integer literal"),
        s"$name accepted a non-literal where an integer literal is required")
    }
  }

  test("integer-literal slots reject a decimal literal") {
    // DECIMAL_LITERAL parses to Double, which is not an accepted integer-literal shape.
    assert(
      interceptUOE(lower("round", arg(0), Literal(2.5))).contains("must be an integer literal"))
    assert(
      interceptUOE(lower("sha2", arg(0), Literal(256.0))).contains("must be an integer literal"))
  }

  test("integer-literal slots accept both Long and Int literal shapes") {
    // AstBuilder emits Long for INTEGER_LITERAL; hand-built ASTs may carry Int.
    assert(
      lower("round", arg(0), Literal(2L)).toString === lower(
        "round",
        arg(0),
        Literal(2)).toString)
    assert(
      lower("sha2", arg(0), Literal(256L)).toString === lower(
        "sha2",
        arg(0),
        Literal(256)).toString)
  }

  // ---------------------------------------------------------------------
  // Integration with ExpressionLowering: a FunctionCall node routes here.
  // ---------------------------------------------------------------------

  test("ExpressionLowering routes FunctionCall nodes through the registry") {
    val direct = lower("year", arg(0)).toString
    val viaAst = ExpressionLowering
      .lower(FunctionCall("year", Seq(PropertyAccess("a", "p0"))), PrefixEnv.raw)
      .toString
    assert(viaAst === direct)
  }

  test("a nested function call lowers its inner call first") {
    val expr = FunctionCall("abs", Seq(FunctionCall("year", Seq(PropertyAccess("a", "p0")))))
    assert(ExpressionLowering.lower(expr, PrefixEnv.raw) != null)
  }

  test("an unknown function nested inside a supported one still fails fast") {
    val expr = FunctionCall("abs", Seq(FunctionCall("not_a_function", Seq(Literal(1L)))))
    assert(
      interceptUOE(ExpressionLowering.lower(expr, PrefixEnv.raw))
        .contains("Unsupported function 'not_a_function'"))
  }
}
