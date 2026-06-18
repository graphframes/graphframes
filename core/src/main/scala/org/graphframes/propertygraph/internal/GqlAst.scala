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
    extends PatternElement

/**
 * `-[variable?:label?]->` or `<-[variable?:label?]-`. Only directed edges exist in v1; the parser
 * rejects undirected `(a)-[e]-(b)`.
 */
private[propertygraph] final case class EdgePattern(
    variable: Option[String],
    label: Option[String],
    direction: Direction)
    extends PatternElement

private[propertygraph] sealed trait Direction
private[propertygraph] case object LeftToRight extends Direction // `-[e]->`
private[propertygraph] case object RightToLeft extends Direction // `<-[e]-`

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
// Precedence mirrors the grammar: OR < AND < NOT < comparison < additive < primary. `Comparison` is
// non-chained (single operator). `Arithmetic` is additive only (`+` / `-`).
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
private[propertygraph] final case class Arithmetic(left: Expression, op: AddOp, right: Expression)
    extends Expression
private[propertygraph] final case class Not(expr: Expression) extends Expression
private[propertygraph] final case class And(left: Expression, right: Expression)
    extends Expression
private[propertygraph] final case class Or(left: Expression, right: Expression) extends Expression

private[propertygraph] sealed trait CompOp
private[propertygraph] case object Eq extends CompOp // `=`
private[propertygraph] case object Neq extends CompOp // `<>` or `!=`
private[propertygraph] case object Lt extends CompOp // `<`
private[propertygraph] case object Lte extends CompOp // `<=`
private[propertygraph] case object Gt extends CompOp // `>`
private[propertygraph] case object Gte extends CompOp // `>=`

private[propertygraph] sealed trait AddOp
private[propertygraph] case object Plus extends AddOp // `+`
private[propertygraph] case object Minus extends AddOp // `-`

private[propertygraph] object GqlAst {

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
