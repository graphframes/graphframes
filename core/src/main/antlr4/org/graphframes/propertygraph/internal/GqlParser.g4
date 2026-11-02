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

parser grammar GqlParser;

options {
    tokenVocab = GqlLexer;
}

// ---------------------------------------------------------------------------
// Top-level statement.
//
// RETURN is optional in the grammar so the engine can default to returning
// matched IDs when the user omits it (forward-compatible; trivially tightened
// later by making RETURN mandatory).
// ---------------------------------------------------------------------------
gqlStatement
    : MATCH matchPattern (WHERE whereClause)? (RETURN returnClause)? EOF
    ;

// A match pattern is a chain of alternating nodes and directed edges,
// e.g. (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company).
matchPattern
    : nodePattern (edgePattern nodePattern)*
    ;

// Node pattern: typed (a:Person), untyped (x), or anonymous ().
nodePattern
    : LPAREN (variable=IDENTIFIER)? (COLON label=IDENTIFIER)? RPAREN
    ;

// Edge pattern.
//
// Three forms:
//   -[e:KNOWS]->     (left-to-right)
//   <-[e:KNOWS]-     (right-to-left)
//   -[:KNOWS]-       (undirected)
// The edge body [variable? :label?] is shared via edgeBody.
edgePattern
    : DASH    edgeBody ARROW_RIGHT   // a -[e]-> b
    | ARROW_LEFT edgeBody DASH       // a <-[e]- b
    | DASH    edgeBody DASH          // a -[e]- b
    ;

edgeBody
    : LBRACK (variable=IDENTIFIER)? (COLON label=IDENTIFIER)? quantifier? RBRACK
    ;

// Variable-length pattern: [KNOWS*1..3] or [KNOWS*3]
quantifier
    : STAR lo=INTEGER_LITERAL DOTDOT hi=INTEGER_LITERAL   // *1..3  (bounded range)
    | STAR exact=INTEGER_LITERAL                          // *3     (exactly 3 hops)
    ;

// ---------------------------------------------------------------------------
// WHERE clause: a single boolean expression.
// ---------------------------------------------------------------------------
whereClause
    : expression
    ;

// ---------------------------------------------------------------------------
// RETURN clause: either SELECT * or a comma-separated list of items.
// ---------------------------------------------------------------------------
returnClause
    : STAR
    | returnItem (COMMA returnItem)*
    ;

returnItem
    : expression (AS alias=IDENTIFIER)?
    ;

// ---------------------------------------------------------------------------
// Expression grammar.
//
// Precedence (lowest -> highest): OR < AND < NOT < comparison < additive <
// multiplicative < primary. Standard recursive-descent shape; ANTLR4 resolves
// left-recursive alternatives correctly.
// ---------------------------------------------------------------------------
expression
    : orExpr
    ;

orExpr
    : andExpr (OR andExpr)*
    ;

andExpr
    : notExpr (AND notExpr)*
    ;

notExpr
    : NOT notExpr
    | comparison
    ;

comparison
    : additive (compOp additive)?
    ;

additive
    : multiplicative ((PLUS | DASH) multiplicative)*
    ;

multiplicative
    : primary ((STAR | SLASH | PERCENT) primary)*
    ;

primary
    : LPAREN expression RPAREN
    | literal
    | functionCall
    | propertyAccess
    | variable=IDENTIFIER
    ;

functionCall
    : name=IDENTIFIER LPAREN ( expression ( COMMA expression )* )? RPAREN
    ;

propertyAccess
    : variable=IDENTIFIER DOT property=IDENTIFIER
    ;

compOp
    : EQ
    | NEQ
    | NEQ_BANG
    | LT
    | LTE
    | GT
    | GTE
    ;

literal
    : INTEGER_LITERAL
    | DECIMAL_LITERAL
    | STRING_LITERAL
    | TRUE
    | FALSE
    | NULL
    ;
