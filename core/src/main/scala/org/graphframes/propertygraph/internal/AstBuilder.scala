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

import org.antlr.v4.runtime.BailErrorStrategy
import org.antlr.v4.runtime.CharStreams
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.RecognitionException
import org.antlr.v4.runtime.misc.ParseCancellationException
import org.antlr.v4.runtime.tree.TerminalNode
import org.graphframes.GraphFramesUnreachableException
import org.graphframes.InvalidParseException

import scala.jdk.CollectionConverters.*

/**
 * Lowers an ANTLR parse tree (produced by the generated `GqlParser`) into the hand-written
 * `GqlAst`. This is the only place in the engine that touches generated `*Context` types. No
 * ANTLR `*Context`/`GqlParser*` type escapes this file.
 */
private[propertygraph] object AstBuilder {

  /** Parse a GQL string into a `MatchStatement`, or throw `InvalidParseException`. */
  def parse(gql: String): MatchStatement = {
    val parsed =
      try {
        val chars = CharStreams.fromString(gql)
        val lexer = new GqlLexer(chars)
        lexer.removeErrorListeners()
        val tokens = new CommonTokenStream(lexer)
        val parser = new GqlParser(tokens)
        parser.removeErrorListeners()
        parser.setErrorHandler(new BailErrorStrategy())
        parser.gqlStatement()
      } catch {
        case e: ParseCancellationException =>
          val cause = Option(e.getCause).map(_.getMessage).getOrElse(e.getMessage)
          throw new InvalidParseException(s"Failed to parse GQL query: $cause")
        case e: RecognitionException =>
          throw new InvalidParseException(s"Failed to parse GQL query: ${e.getMessage}")
      }
    // visit returns AnyRef (visitGqlStatement returns MatchStatement).
    new AstBuilder().visit(parsed).asInstanceOf[MatchStatement]
  }
}

// Visitor type is AnyRef because ANTLR's generic T must be a reference type, and different rules
// produce different node kinds (MatchStatement, PatternElement, Expression, ...).
private[propertygraph] final class AstBuilder extends GqlParserBaseVisitor[AnyRef] {

  override def visitGqlStatement(ctx: GqlParser.GqlStatementContext): MatchStatement = {
    val pattern = visitMatchPattern(ctx.matchPattern())
    val where = Option(ctx.whereClause()).map(c => visitExpression(c.expression()))
    val returnClause = Option(ctx.returnClause()).map(visitReturnClause)
    MatchStatement(pattern, where, returnClause)
  }

  // matchPattern: nodePattern (edgePattern nodePattern)*
  // Reinterleave into the user-written order: N0, E0, N1, E1, N2, ...
  override def visitMatchPattern(ctx: GqlParser.MatchPatternContext): GraphPattern = {
    val nodes = ctx.nodePattern().asScala.map(visitNodePattern)
    val edges = ctx.edgePattern().asScala.map(visitEdgePattern)
    val elements = scala.collection.mutable.ListBuffer.empty[PatternElement]
    elements += nodes.head
    edges.zip(nodes.tail).foreach { case (e, n) =>
      elements += e
      elements += n
    }
    GraphPattern(elements.toSeq)
  }

  // nodePattern: LPAREN (variable=IDENTIFIER)? (COLON label=IDENTIFIER)? RPAREN
  // The generated context exposes IDENTIFIERs as a flat list; use COLON presence to disambiguate.
  override def visitNodePattern(ctx: GqlParser.NodePatternContext): NodePattern = {
    val (variable, label) =
      readVariableLabel(ctx.IDENTIFIER().asScala.map(_.getText).toSeq, ctx.COLON())
    NodePattern(variable, label)
  }

  // edgePattern: DASH edgeBody ARROW_RIGHT | ARROW_LEFT edgeBody DASH
  override def visitEdgePattern(ctx: GqlParser.EdgePatternContext): EdgePattern = {
    val direction = if (ctx.ARROW_RIGHT() != null) LeftToRight else RightToLeft
    val (variable, label) =
      readVariableLabel(
        ctx.edgeBody().IDENTIFIER().asScala.map(_.getText).toSeq,
        ctx.edgeBody().COLON())
    EdgePattern(variable, label, direction)
  }

  override def visitWhereClause(ctx: GqlParser.WhereClauseContext): Expression =
    visitExpression(ctx.expression())

  override def visitReturnClause(ctx: GqlParser.ReturnClauseContext): ReturnClause = {
    if (ctx.STAR() != null) {
      ReturnStar
    } else {
      ReturnItems(ctx.returnItem().asScala.map(visitReturnItem).toSeq)
    }
  }

  override def visitReturnItem(ctx: GqlParser.ReturnItemContext): ReturnItem = {
    val expr = visitExpression(ctx.expression())
    val alias = Option(ctx.IDENTIFIER()).map(_.getText)
    ReturnItem(expr, alias)
  }

  // -------------------------------------------------------------------------
  // Expression tier. Each level folds left-associative chains; NOT is prefix
  // and stacks (NOT NOT a > b).
  // -------------------------------------------------------------------------

  override def visitExpression(ctx: GqlParser.ExpressionContext): Expression =
    visitOrExpr(ctx.orExpr())

  override def visitOrExpr(ctx: GqlParser.OrExprContext): Expression = {
    val parts = ctx.andExpr().asScala.map(visitAndExpr)
    parts.reduceLeftOption(Or.apply).getOrElse(Literal(true))
  }

  override def visitAndExpr(ctx: GqlParser.AndExprContext): Expression = {
    val parts = ctx.notExpr().asScala.map(visitNotExpr)
    parts.reduceLeftOption(And.apply).getOrElse(Literal(true))
  }

  override def visitNotExpr(ctx: GqlParser.NotExprContext): Expression = {
    if (ctx.NOT() != null) {
      Not(visitNotExpr(ctx.notExpr()))
    } else {
      visitComparison(ctx.comparison())
    }
  }

  override def visitComparison(ctx: GqlParser.ComparisonContext): Expression = {
    val left = visitAdditive(ctx.additive(0))
    if (ctx.compOp() != null) {
      val op = visitCompOp(ctx.compOp())
      val right = visitAdditive(ctx.additive(1))
      Comparison(left, op, right)
    } else {
      left
    }
  }

  // additive: primary ((PLUS | DASH) primary)*  (left-associative)
  // ANTLR exposes PLUS and DASH as two separate token lists, which loses their interleaving. We
  // instead walk the context's children in source order so operators stay aligned with primaries.
  override def visitAdditive(ctx: GqlParser.AdditiveContext): Expression = {
    val ops: Seq[AddOp] = ctx.children.asScala.collect {
      case t if t.getText == "+" => Plus: AddOp
      case t if t.getText == "-" => Minus: AddOp
    }.toSeq
    val primaries = ctx.primary().asScala.map(visitPrimary).toSeq
    primaries.tail.zip(ops).foldLeft(primaries.head: Expression) { case (acc, (rhs, op)) =>
      Arithmetic(acc, op, rhs)
    }
  }

  override def visitPrimary(ctx: GqlParser.PrimaryContext): Expression = {
    if (ctx.LPAREN() != null) {
      visitExpression(ctx.expression())
    } else if (ctx.literal() != null) {
      visitLiteral(ctx.literal())
    } else if (ctx.propertyAccess() != null) {
      visitPropertyAccess(ctx.propertyAccess())
    } else {
      Variable(ctx.IDENTIFIER().getText)
    }
  }

  override def visitPropertyAccess(ctx: GqlParser.PropertyAccessContext): Expression = {
    val ids = ctx.IDENTIFIER().asScala.map(_.getText)
    // propertyAccess: variable=IDENTIFIER DOT property=IDENTIFIER -> exactly two identifiers.
    PropertyAccess(ids(0), ids(1))
  }

  override def visitCompOp(ctx: GqlParser.CompOpContext): CompOp = {
    if (ctx.EQ() != null) Eq
    else if (ctx.NEQ() != null || ctx.NEQ_BANG() != null) Neq
    else if (ctx.LT() != null) Lt
    else if (ctx.LTE() != null) Lte
    else if (ctx.GT() != null) Gt
    else if (ctx.GTE() != null) Gte
    else {
      throw new GraphFramesUnreachableException()
    }
  }

  override def visitLiteral(ctx: GqlParser.LiteralContext): Literal = {
    val value: Any =
      if (ctx.INTEGER_LITERAL() != null) {
        ctx.INTEGER_LITERAL().getText.toLong
      } else if (ctx.DECIMAL_LITERAL() != null) {
        ctx.DECIMAL_LITERAL().getText.toDouble
      } else if (ctx.STRING_LITERAL() != null) {
        unquoteString(ctx.STRING_LITERAL().getText)
      } else if (ctx.TRUE() != null) {
        java.lang.Boolean.TRUE
      } else if (ctx.FALSE() != null) {
        java.lang.Boolean.FALSE
      } else {
        null // NULL
      }
    Literal(value)
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /**
   * Recover `(variable, label)` from a node/edge body's identifier list.
   *
   * Grammar shapes (both `(variable)? (COLON label)?`):
   *   - 0 identifiers, no colon -> `(None, None)`
   *   - 1 identifier, no colon -> `(Some(var), None)`
   *   - 1 identifier, colon -> `(None, Some(label))` (label-only, e.g. `(:Person)`)
   *   - 2 identifiers, colon -> `(Some(var), Some(label))`
   */
  private def readVariableLabel(
      ids: Seq[String],
      colon: TerminalNode): (Option[String], Option[String]) = {
    if (colon == null) {
      (ids.headOption, None)
    } else {
      ids.size match {
        case 1 => (None, Some(ids.head))
        case 2 => (Some(ids.head), Some(ids(1)))
        case _ => (None, None) // unreachable given the grammar
      }
    }
  }

  /** Strip the surrounding single quotes and undo the `''` escape. */
  private def unquoteString(raw: String): String = {
    val withoutQuotes = raw.substring(1, raw.length - 1)
    withoutQuotes.replace("''", "'")
  }
}
