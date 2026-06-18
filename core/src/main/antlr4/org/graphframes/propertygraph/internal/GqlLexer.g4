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

lexer grammar GqlLexer;

// ---------------------------------------------------------------------------
// Keywords (case-insensitive). These MUST precede IDENTIFIER so that, on a
// tie, the keyword token wins over the identifier rule.
// ---------------------------------------------------------------------------
MATCH:   M A T C H;
WHERE:   W H E R E;
RETURN:  R E T U R N;
AND:     A N D;
OR:      O R;
NOT:     N O T;
AS:      A S;
TRUE:    T R U E;
FALSE:   F A L S E;
NULL:    N U L L;
IS:      I S;
IN:      I N;

// ---------------------------------------------------------------------------
// Punctuation and operators.
//
// ANTLR4 uses maximal munch: the longest match always wins, and on ties the
// rule listed first wins. Multi-character tokens are therefore safe even when
// they share a prefix with a shorter one (e.g. '->' beats '-' regardless of
// ordering), but equal-length alternatives must be ordered with care. None of
// the tokens below collide on (length, prefix), so ordering within a group is
// not load-bearing; groups are kept before IDENTIFIER for clarity only.
// ---------------------------------------------------------------------------
ARROW_RIGHT: '->';
ARROW_LEFT:  '<-';
LTE:         '<=';
GTE:         '>=';
NEQ:         '<>';
NEQ_BANG:    '!=';
LT:          '<';
GT:          '>';
EQ:          '=';
DASH:        '-';
PLUS:        '+';
STAR:        '*';
DOT:         '.';
COMMA:       ',';
COLON:       ':';
LPAREN:      '(';
RPAREN:      ')';
LBRACK:      '[';
RBRACK:      ']';
LBRACE:      '{';
RBRACE:      '}';

// ---------------------------------------------------------------------------
// Literals
// ---------------------------------------------------------------------------

// Single-quoted string with '' escape, per SQL/GQL convention.
STRING_LITERAL: '\'' ( ~'\'' | '\'\'' )* '\'';

DECIMAL_LITERAL: DIGIT+ '.' DIGIT+ | DIGIT+ '.' | '.' DIGIT+;
INTEGER_LITERAL: DIGIT+;

// ---------------------------------------------------------------------------
// Identifiers. Must follow all keyword rules so keywords win the tie.
// ---------------------------------------------------------------------------
IDENTIFIER: [a-zA-Z_] [a-zA-Z0-9_]*;

// ---------------------------------------------------------------------------
// Whitespace and comments -> skipped.
// ---------------------------------------------------------------------------
WS:            [ \t\r\n\u000C]+ -> skip;
LINE_COMMENT:  '//' ~[\r\n]*       -> skip;
BLOCK_COMMENT: '/*' .*? '*/'        -> skip;

// ---------------------------------------------------------------------------
// Fragments
// ---------------------------------------------------------------------------
fragment DIGIT: [0-9];

// Letter fragments used to build case-insensitive keywords.
fragment A: ('a' | 'A');
fragment B: ('b' | 'B');
fragment C: ('c' | 'C');
fragment D: ('d' | 'D');
fragment E: ('e' | 'E');
fragment F: ('f' | 'F');
fragment G: ('g' | 'G');
fragment H: ('h' | 'H');
fragment I: ('i' | 'I');
fragment J: ('j' | 'J');
fragment K: ('k' | 'K');
fragment L: ('l' | 'L');
fragment M: ('m' | 'M');
fragment N: ('n' | 'N');
fragment O: ('o' | 'O');
fragment P: ('p' | 'P');
fragment Q: ('q' | 'Q');
fragment R: ('r' | 'R');
fragment S: ('s' | 'S');
fragment T: ('t' | 'T');
fragment U: ('u' | 'U');
fragment V: ('v' | 'V');
fragment W: ('w' | 'W');
fragment X: ('x' | 'X');
fragment Y: ('y' | 'Y');
fragment Z: ('z' | 'Z');
