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

/**
 * Hand-written AST for the GQL subset. This is the firewall between ANTLR-generated parse-tree
 * types (which appear only inside `AstBuilder`) and the rest of the engine. Nothing in `GqlAst`
 * references an ANTLR `*Context` type.
 */
private[propertygraph] sealed trait GqlStatement

/** The single statement form supported is `MATCH <pattern> [WHERE ...] [RETURN ...]`. */
private[propertygraph] final case class MatchStatement(
    pattern: GraphPattern,
    where: Option[Expression],
    returnClause: Option[ReturnClause])
    extends GqlStatement

/** A linear chain of `NodePattern` / `EdgePattern` elements as written by the user. */
private[propertygraph] final case class GraphPattern(elements: Seq[PatternElement])

private[propertygraph] sealed trait PatternElement

/** `(variable?:label?)`. Both fields optional: `(a:Person)`, `(x)`, `()`, `(:Person)`. */
private[propertygraph] final case class NodePattern(
    variable: Option[String],
    label: Option[String])
    extends PatternElement {

  override def toString: String = {
    val innerPart = variable.getOrElse("") + label.map(f => s":$f").getOrElse("")
    s"($innerPart)"
  }
}

/**
 * `-[variable?:label?]->` or `<-[variable?:label?]-`. Directed, undirected edges. Variable-length
 * edges.
 */
private[propertygraph] final case class EdgePattern(
    variable: Option[String],
    label: Option[String],
    direction: Direction,
    hopsRange: Option[(Int, Int)])
    extends PatternElement {

  override def toString: String = {
    val varPart = variable.map(v => s"$v").getOrElse("")
    val labelPart = label.map(l => s":$l").getOrElse("")
    val edgeCore = s"$varPart$labelPart"
    val hopsPart = hopsRange match {
      case Some((lo, hi)) if lo == hi && lo == 1 => ""
      case Some((lo, hi)) if lo == hi => s"*$lo"
      case Some((lo, hi)) => s"*$lo..$hi"
      case None => ""
    }
    val edgeStr = s"-[$edgeCore$hopsPart]-"
    direction match {
      case LeftToRight => s"$edgeStr>"
      case RightToLeft => s"<$edgeStr"
      case Undirected => edgeStr
    }
  }
}
private[propertygraph] sealed trait Direction
private[propertygraph] case object LeftToRight extends Direction // `-[e]->`
private[propertygraph] case object RightToLeft extends Direction // `<-[e]-`
private[propertygraph] case object Undirected extends Direction // `-[e]-`

// ---------------------------------------------------------------------------
// RETURN clause
// ---------------------------------------------------------------------------

private[propertygraph] sealed trait ReturnClause
private[propertygraph] case object ReturnStar extends ReturnClause
private[propertygraph] final case class ReturnItems(items: Seq[ReturnItem]) extends ReturnClause

private[propertygraph] final case class ReturnItem(expression: Expression, alias: Option[String])

// ---------------------------------------------------------------------------
// Expressions (shared by WHERE and RETURN).
//
// Precedence mirrors the grammar: OR < AND < NOT < comparison < additive <
// multiplicative < primary. `Comparison` is non-chained (single operator).
// `Arithmetic` covers additive (`+`/`-`) and multiplicative (`*`/`/`/`%`) ops;
// precedence is encoded in the grammar tier, not in the node.
// ---------------------------------------------------------------------------

private[propertygraph] sealed trait Expression

private[propertygraph] final case class Literal(value: Any) extends Expression
private[propertygraph] final case class Variable(name: String) extends Expression
private[propertygraph] final case class PropertyAccess(variable: String, property: String)
    extends Expression
private[propertygraph] final case class Comparison(
    left: Expression,
    op: CompOp,
    right: Expression)
    extends Expression
private[propertygraph] final case class Arithmetic(
    left: Expression,
    op: ArithOp,
    right: Expression)
    extends Expression
private[propertygraph] final case class Not(expr: Expression) extends Expression
private[propertygraph] final case class And(left: Expression, right: Expression)
    extends Expression
private[propertygraph] final case class Or(left: Expression, right: Expression) extends Expression
private[propertygraph] final case class FunctionCall(name: String, args: Seq[Expression])
    extends Expression

private[propertygraph] sealed trait CompOp
private[propertygraph] case object Eq extends CompOp // `=`
private[propertygraph] case object Neq extends CompOp // `<>` or `!=`
private[propertygraph] case object Lt extends CompOp // `<`
private[propertygraph] case object Lte extends CompOp // `<=`
private[propertygraph] case object Gt extends CompOp // `>`
private[propertygraph] case object Gte extends CompOp // `>=`

// Arithmetic operators. `AddOp` (+/-) and `MulOp` (*///%) are subtraits of a common
// `ArithOp` so the `Arithmetic` node carries one op type; precedence is encoded in the grammar
// (multiplicative binds tighter than additive), not here.
private[propertygraph] sealed trait ArithOp
private[propertygraph] sealed trait AddOp extends ArithOp
private[propertygraph] case object Plus extends AddOp // `+`
private[propertygraph] case object Minus extends AddOp // `-`
private[propertygraph] sealed trait MulOp extends ArithOp
private[propertygraph] case object Mult extends MulOp // `*`
private[propertygraph] case object Div extends MulOp // `/` (floating-point division)
private[propertygraph] case object Mod extends MulOp // `%` (sign follows the dividend)

private[propertygraph] object GqlAst {

  /**
   * `id(n)`: the standardized (masked) id of the node bound to `n`. Unlike `n.id`, which is the
   * raw property named `id`, this is the value the engine joins on.
   */
  val IdFunction: String = "id"

  /**
   * `weight(e)`: the standardized weight of the edge bound to `e` (the group's weight column,
   * whatever its raw name). Unlike `e.weight`, which is the raw property named `weight`.
   */
  val WeightFunction: String = "weight"

  /**
   * True when `expr` reads a standardized node column -- a bare variable (its id) or `id(n)`.
   * Such a column only exists after `getData` standardizes the group, so a conjunct containing it
   * cannot be lowered against the raw group data as a scan-local filter.
   */
  def readsStandardizedNodeColumn(expr: Expression): Boolean = expr match {
    case Variable(_) => true
    case FunctionCall(name, _) if name.equalsIgnoreCase(IdFunction) => true
    case PropertyAccess(_, _) | Literal(_) => false
    case Comparison(l, _, r) => readsStandardizedNodeColumn(l) || readsStandardizedNodeColumn(r)
    case Arithmetic(l, _, r) => readsStandardizedNodeColumn(l) || readsStandardizedNodeColumn(r)
    case Not(e) => readsStandardizedNodeColumn(e)
    case And(l, r) => readsStandardizedNodeColumn(l) || readsStandardizedNodeColumn(r)
    case Or(l, r) => readsStandardizedNodeColumn(l) || readsStandardizedNodeColumn(r)
    case FunctionCall(_, args) => args.exists(readsStandardizedNodeColumn)
  }

  /** Collect every variable name referenced anywhere in `expr`. */
  def referencedVariables(expr: Expression): Set[String] = expr match {
    case Variable(name) => Set(name)
    case PropertyAccess(variable, _) => Set(variable)
    case Literal(_) => Set.empty
    case Comparison(l, _, r) => referencedVariables(l) ++ referencedVariables(r)
    case Arithmetic(l, _, r) => referencedVariables(l) ++ referencedVariables(r)
    case Not(e) => referencedVariables(e)
    case And(l, r) => referencedVariables(l) ++ referencedVariables(r)
    case Or(l, r) => referencedVariables(l) ++ referencedVariables(r)
    case FunctionCall(_, args) => args.flatMap(referencedVariables).toSet
  }

  /**
   * Split a boolean expression on top-level `AND` into its conjuncts. Used by the resolver to
   * classify WHERE predicates individually (scan-local vs join vs post-join).
   */
  def flattenAnd(expr: Expression): Seq[Expression] = expr match {
    case And(l, r) => flattenAnd(l) ++ flattenAnd(r)
    case other => Seq(other)
  }
}
