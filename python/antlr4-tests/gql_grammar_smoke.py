#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
Smoke test for the minimal GQL (ISO/IEC 39075 subset) ANTLR4 grammar.

This is a developer/CI testing tool, NOT a shipped runtime component. It does
not depend on Spark/JVM and runs against the pure-Python ANTLR4 runtime.

What it does
------------
1. Locates the two grammar files under
   ``core/src/main/antlr4/org/graphframes/propertygraph/internal/``
   (``GqlLexer.g4`` and ``GqlParser.g4``).
2. Generates the Python3 parser/lexer into a fresh *temporary* directory using
   the ``antlr4`` CLI provided by the ``antlr4-tools`` dev dependency. Generated
   sources are deliberately NOT committed to the repository -- they are
   regenerated on every run so the test always reflects the current grammar.
3. Imports the freshly generated modules and parses each query in
   :data:`CASES`, asserting that the grammar accepts/rejects each one as
   expected. Cases mirror the v1 scope in ``GQL_GF_PROPOSAL_v1.md`` (section
   2.1 accept / 2.2 reject).

Usage
-----
    cd python && poetry run python antlr4-tests/gql_grammar_smoke.py

Exits non-zero on any failure so it can gate CI.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from antlr4 import CommonTokenStream, InputStream
from antlr4.error.Errors import ParseCancellationException, RecognitionException
from antlr4.error.ErrorStrategy import BailErrorStrategy

# The grammar is under the Scala ``core`` module. Resolve it relative to this
# file so the script works regardless of the current working directory.
REPO_ROOT = Path(__file__).resolve().parents[2]
GRAMMAR_DIR = (
    REPO_ROOT
    / "core"
    / "src"
    / "main"
    / "antlr4"
    / "org"
    / "graphframes"
    / "propertygraph"
    / "internal"
)
LEXER_G4 = GRAMMAR_DIR / "GqlLexer.g4"
PARSER_G4 = GRAMMAR_DIR / "GqlParser.g4"

# (query, should_accept). Keep aligned with GQL_GF_PROPOSAL_v1.md sections 2.1/2.2.
CASES: list[tuple[str, bool]] = [
    # --- proposal 2.1: must accept ---
    ("MATCH (a:Person)", True),
    ("MATCH (a:Person)-[:KNOWS]->(b:Person)", True),
    ("MATCH (x)", True),  # untyped node
    ("MATCH (a:Person)-[]->(b:Person)", True),  # anonymous edge
    ("MATCH (a)-[e]->(b)", True),  # untyped edge with variables
    ("MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company)", True),  # multi-hop
    ("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 30 RETURN a, b", True),
    ("MATCH (a:Person) WHERE a.age > b.age RETURN a, b", True),  # cross-pattern filter
    ("MATCH (a:Person) RETURN a.name AS person_name", True),  # aliased property
    ("MATCH (a:Person) RETURN a, b", True),
    ("MATCH (a:Person) RETURN *", True),
    # --- extras within the v1 subset ---
    ("MATCH (a:Person) WHERE NOT (a.age > 30) RETURN a", True),
    ("MATCH (a:Person) WHERE a.age > 30 AND a.age < 90 OR a.age = 100 RETURN a", True),
    ("MATCH (a:Person) WHERE a.name = 'Bob''s' RETURN a", True),  # '' escape
    ("MATCH (a:Person) WHERE a.active = TRUE AND a.score = 3.14 RETURN a", True),
    ("MATCH (a:Person) WHERE a.age + 1 > 30 RETURN a", True),  # additive
    ("match (a:Person) where a.age > 30 return a", True),  # case-insensitive keywords
    ("MATCH (a:Person) // line comment\n RETURN a", True),
    ("MATCH /* block */ (a:Person) RETURN a", True),
    ("MATCH (a:Person) WHERE a.age <> 30 AND a.x != 1 RETURN a", True),
    ("MATCH (a:Person)", True),  # RETURN is grammar-optional
    # --- proposal 2.2: must reject (out-of-scope constructs) ---
    ("MATCH (a:Person)-[:KNOWS*1..5]->(b:Person) RETURN a, b", False),  # var-length path
    ("OPTIONAL MATCH (a:Person) RETURN a", False),  # OPTIONAL MATCH
    ("MATCH (a:Person) WITH a RETURN a", False),  # WITH
    ("MATCH (a:Person) RETURN count(a)", False),  # aggregate function call
    ("MATCH (a:Person)-[:KNOWS]-(b:Person) RETURN a", False),  # undirected edge
    ("MATCH (a:Person) RETURN a ORDER BY a.name", False),  # ORDER BY
    ("MATCH (a:Person) RETURN a LIMIT 10", False),  # LIMIT
    ("MATCH (a:Person)-[e:KNOWS]-> WHERE x = 1 RETURN a", False),  # missing dst node
]


def _generate_parser(gen_dir: Path) -> None:
    """Run the antlr4 CLI to emit Python3 sources for both grammar files."""
    if not LEXER_G4.is_file() or not PARSER_G4.is_file():
        raise FileNotFoundError(f"Grammar files missing under {GRAMMAR_DIR}")
    # The parser imports the lexer's token vocab, so generate the lexer first.
    # antlr4 emits next to the .g4 by default; -o controls the output dir.
    for grammar in (LEXER_G4, PARSER_G4):
        result = subprocess.run(
            ["antlr4", "-Dlanguage=Python3", "-o", str(gen_dir), str(grammar)],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"antlr4 failed on {grammar.name} (exit {result.returncode}):\n"
                f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )


def _load_generated(gen_dir: Path):
    """Import the freshly generated GqlLexer/GqlParser modules."""
    # With the Python target, `antlr4 -o <dir>` emits the generated .py files
    # directly into <dir> (flat, not under the original package path).
    sys.path.insert(0, str(gen_dir))
    # Imported after sys.path mutation on purpose; flake8 E402 is ignored repo-wide.
    from GqlLexer import GqlLexer  # noqa: E402
    from GqlParser import GqlParser  # noqa: E402

    return GqlLexer, GqlParser


def _accepts(GqlLexer, GqlParser, text: str) -> bool:
    """Return True iff `text` parses cleanly as a full gqlStatement."""
    lexer = GqlLexer(InputStream(text))
    # Bail on the first lexer error: a recoverable lexer error means we do not
    # fully accept the input, which is what we want for accept/reject testing.
    lexer.removeErrorListeners()  # silence noise; we detect via exception
    stream = CommonTokenStream(lexer)
    parser = GqlParser(stream)
    parser.removeErrorListeners()
    parser._errHandler = BailErrorStrategy()
    try:
        parser.gqlStatement()
    except (ParseCancellationException, RecognitionException):
        return False
    # Reject trailing tokens: the start rule must consume the whole input.
    return stream.index == len(stream.tokens) - 1


def main() -> int:
    if shutil.which("antlr4") is None:
        print(
            "ERROR: 'antlr4' CLI not found. Run via: cd python && poetry run python "
            "antlr4-tests/gql_grammar_smoke.py",
            file=sys.stderr,
        )
        return 2

    with tempfile.TemporaryDirectory(prefix="gql-grammar-gen-") as tmp:
        gen_dir = Path(tmp)
        _generate_parser(gen_dir)
        GqlLexer, GqlParser = _load_generated(gen_dir)

        passed = 0
        failed = 0
        for query, expect_accept in CASES:
            got_accept = _accepts(GqlLexer, GqlParser, query)
            ok = got_accept == expect_accept
            tag = "OK  " if ok else "FAIL"
            if ok:
                passed += 1
            else:
                failed += 1
            want = "accept" if expect_accept else "reject"
            got = "accept" if got_accept else "reject"
            detail = "" if ok else f"  <- wanted {want}, got {got}"
            print(f"[{tag}] want={want:6} got={got:6} | {query!r}{detail}")

        print(f"\n{passed} passed, {failed} failed, {len(CASES)} total")
        return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
