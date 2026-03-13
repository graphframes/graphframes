# Pregel Tutorial Verification Results

**Date**: 2026-03-13
**Tutorial**: `docs/src/03-tutorials/04-pregel-tutorial.md`
**Code**: `python/graphframes/tutorials/pregel.py`

## Summary

✅ **PASS** — All 7 tutorial examples run end-to-end successfully across all 4 environment combinations with the published `graphframes-py==0.10.1` package from PyPI.

## Base Test Environment

| Component | Version |
|-----------|---------|
| Python | 3.12 |
| Java | OpenJDK 17.0.18 |
| PySpark | 4.0.2 |
| graphframes-py | 0.10.1 (PyPI) |
| Scala | 2.13 (via JVM core) |
| OS | Ubuntu 24.04 (Linux 6.1.147) |

## All 4 Environment Combinations

### ✅ Combination 1: venv + uv

```bash
python3 -m venv /tmp/test-venv-uv
source /tmp/test-venv-uv/bin/activate
uv pip install "pyspark>=4.0,<4.1" "graphframes-py>=0.10.1" numpy typing_extensions click py7zr requests
spark-submit --packages io.graphframes:graphframes-spark4_2.13:0.10.1 \
    --driver-memory 4g --executor-memory 4g python/graphframes/tutorials/pregel.py
```

**Result**: All 7 examples PASS. Rank correlation: 0.9999.

### ✅ Combination 2: venv + poetry/pip

```bash
python3 -m venv /tmp/test-venv-pip
source /tmp/test-venv-pip/bin/activate
pip install "pyspark>=4.0,<4.1" "graphframes-py>=0.10.1" typing_extensions click py7zr requests
spark-submit --packages io.graphframes:graphframes-spark4_2.13:0.10.1 \
    --driver-memory 4g --executor-memory 4g python/graphframes/tutorials/pregel.py
```

**Result**: All 7 examples PASS. Rank correlation: 0.9999.

### ✅ Combination 3: conda + uv

```bash
conda create -n test-conda-uv python=3.12 -y
conda activate test-conda-uv
uv pip install "pyspark>=4.0,<4.1" "graphframes-py>=0.10.1" numpy typing_extensions click py7zr requests
spark-submit --packages io.graphframes:graphframes-spark4_2.13:0.10.1 \
    --driver-memory 4g --executor-memory 4g python/graphframes/tutorials/pregel.py
```

**Result**: All 7 examples PASS. Rank correlation: 0.9999.

### ✅ Combination 4: conda + poetry/pip

```bash
conda create -n test-conda-poetry python=3.12 -y
conda activate test-conda-poetry
pip install "pyspark>=4.0,<4.1" "graphframes-py>=0.10.1" numpy typing_extensions click py7zr requests
spark-submit --packages io.graphframes:graphframes-spark4_2.13:0.10.1 \
    --driver-memory 4g --executor-memory 4g python/graphframes/tutorials/pregel.py
```

**Result**: All 7 examples PASS. Rank correlation: 1.0000.

## Per-Example Results (Consistent Across All Environments)

| Example | Status | Key Result |
|---------|--------|------------|
| 1. In-Degree (AggregateMessages) | ✅ PASS | 81,735 zero-degree nodes, top node: 143 in-degree |
| 2. In-Degree (Pregel) | ✅ PASS | Identical results to AggregateMessages |
| 3. PageRank | ✅ PASS | Rank correlation with built-in: 0.9999-1.0000 |
| 4. Connected Components | ✅ PASS | 40,115 components found |
| 5. Shortest Paths | ✅ PASS | 56,442/129,751 (43.5%) reachable from source |
| 6. Reputation Propagation | ✅ PASS | 42,712 vertices, 5,745 edges in subgraph |
| 7. Debug Trace | ✅ PASS | Path traces displayed correctly |

## Data Setup Verification

```bash
# Download Stack Exchange data
graphframes stackexchange stats.meta

# Convert XML to Parquet (Spark 4.0 has built-in XML)
spark-submit --driver-memory 4g --executor-memory 4g python/graphframes/tutorials/stackexchange.py
```

✅ Both steps complete successfully. No spark-xml package needed with Spark 4.0.

## Mermaid Diagrams

All 8 SVG diagrams rendered successfully using `mmdc` (PhantomJS-based Mermaid renderer):

| Diagram | File Size | Status |
|---------|-----------|--------|
| pregel-bsp-model.svg | 15,390 | ✅ OK |
| pregel-in-degree-am.svg | 13,143 | ✅ OK |
| pregel-in-degree-pregel.svg | 16,445 | ✅ OK |
| pregel-pagerank-iterations.svg | 15,460 | ✅ OK |
| pregel-connected-components.svg | 6,459 | ✅ OK |
| pregel-shortest-paths.svg | 11,662 | ✅ OK |
| pregel-reputation-propagation.svg | 16,987 | ✅ OK |
| pregel-debug-trace.svg | 10,975 | ✅ OK |

## Docs Build

✅ `build/sbt "docs / laikaHTML"` completes successfully with zero errors.

## Known Issues

1. **`tutorials` extra not in PyPI 0.10.1**: The `[tutorials]` extra was added to the source after the 0.10.1 release. Users must install tutorial dependencies manually: `pip install click py7zr requests`.

2. **`numpy` not auto-installed**: Some environments don't get `numpy` via PySpark's dependencies. Install it explicitly if needed.

3. **PageRank normalization**: The built-in `g.pageRank()` and our Pregel implementation use different normalization, producing different absolute values. The rank correlation is 0.9999-1.0000, confirming rankings match.

## Required Packages

For any environment (conda or venv, with uv or pip/poetry):

```bash
pip install "pyspark>=4.0,<4.1" "graphframes-py>=0.10.1" numpy typing_extensions click py7zr requests
```

Plus the JVM core at runtime:
```bash
--packages io.graphframes:graphframes-spark4_2.13:0.10.1
```
