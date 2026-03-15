"""Generate Mermaid diagrams for the Pregel and Motif Finding tutorials.

Usage:
    python python/graphframes/tutorials/generate_diagrams.py

Renders Mermaid diagram source to SVG using the mmdc package (PhantomJS-based).
"""

import os
import re
from pathlib import Path

from mmdc import MermaidConverter

# Resolve repo root relative to this file so the script works regardless of cwd
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
_IMG_ROOT = _REPO_ROOT / "docs" / "src" / "img"

converter = MermaidConverter(timeout=30)


def save_diagram(name: str, mermaid_code: str, output_dir: Path, width: int = 900) -> None:
    """Render a Mermaid diagram to SVG via mmdc, then thicken edges."""
    output_dir.mkdir(parents=True, exist_ok=True)
    svg_path = output_dir / f"{name}.svg"
    try:
        converter.convert(
            mermaid_code.strip(),
            output_file=svg_path,
            theme="default",
            background="white",
            width=width,
        )
        # Post-process SVG to thicken edges (3x default ~1px)
        # Use regex with negative lookahead to avoid mutating multi-digit values
        # (e.g., stroke-width:10 should NOT become stroke-width:30)
        svg_text = svg_path.read_text()
        svg_text = re.sub(r"stroke-width\s*:\s*2(?!\d)", "stroke-width:6", svg_text)
        svg_text = re.sub(r"stroke-width\s*:\s*1(?!\d)", "stroke-width:3", svg_text)
        svg_path.write_text(svg_text)
        print(f"Saved: {svg_path}")
    except Exception as e:
        print(f"Error rendering {name}: {e}")
        mmd_path = output_dir / f"{name}.mmd"
        mmd_path.write_text(mermaid_code.strip())
        print(f"Saved Mermaid source: {mmd_path}")


# ══════════════════════════════════════════════════════════════════════════════
# PREGEL TUTORIAL DIAGRAMS  →  docs/src/img/pregel-diagrams/
# ══════════════════════════════════════════════════════════════════════════════

# ──────────────────────────────────────────────────────────────────────
# 1. BSP Model Overview
# ──────────────────────────────────────────────────────────────────────
bsp_model = """
graph LR
    C0["Compute 0"] --> M0["Send Messages 0"]
    M0 --> B0["Barrier 0"]
    B0 --> C1["Compute 1"]
    C1 --> M1["Send Messages 1"]
    M1 --> B1["Barrier 1"]
    B1 --> C2["Compute 2"]
    C2 --> M2["Send Messages 2"]
    M2 --> B2["Converged - Halt"]
"""

# ──────────────────────────────────────────────────────────────────────
# 2. In-Degree with AggregateMessages
# ──────────────────────────────────────────────────────────────────────
in_degree_am = """
graph LR
    A((A)) -->|"1"| B((B))
    A -->|"1"| C((C))
    B -->|"1"| C
    C -->|"1"| D((D))
    B -->|"sum=1"| B2["B: in=1"]
    C -->|"sum=2"| C2["C: in=2"]
    D -->|"sum=1"| D2["D: in=1"]
"""

# ──────────────────────────────────────────────────────────────────────
# 3. In-Degree with Pregel (superstep view)
# ──────────────────────────────────────────────────────────────────────
in_degree_pregel = """
graph TB
    A0["A: init=0"] -->|"sends 1"| B1["B"]
    A0 -->|"sends 1"| C1["C"]
    B0["B: init=0"] -->|"sends 1"| C1
    C0["C: init=0"] -->|"sends 1"| D1["D"]
    D0["D: init=0"]
    B1 --> BR["B: sum=1"]
    C1 --> CR["C: sum=2"]
    D1 --> DR["D: sum=1"]
    A0 --> AR["A: sum=0"]
"""

# ──────────────────────────────────────────────────────────────────────
# 4. PageRank Iterations
# ──────────────────────────────────────────────────────────────────────
pagerank_iterations = """
graph LR
    A0["A: PR=0.25 out=2"] -->|"0.125"| B0["B: PR=0.25 out=1"]
    A0 -->|"0.125"| C0["C: PR=0.25 out=1"]
    B0 -->|"0.250"| C0
    C0 -->|"0.250"| D0["D: PR=0.25 out=0"]
    B0 -.-> B1["B: 0.14"]
    C0 -.-> C1["C: 0.36"]
    D0 -.-> D1["D: 0.25"]
    A0 -.-> A1["A: 0.04"]
"""

# ──────────────────────────────────────────────────────────────────────
# 5. Connected Components
# ──────────────────────────────────────────────────────────────────────
# Strategy: three side-by-side superstep panels (graph TB makes
# unconnected subgraphs lay out left→right) showing the minimum-label
# "wave" advancing one hop per superstep across two separate components
# ({1,2,3} and {4,5}).  Node labels use vertex:componentLabel notation.
# Note: chained undirected edges (A --- B --- C) fail in some Mermaid
# versions; use explicit two-node edge lines instead.
connected_components = """
graph TB
    subgraph s0 Superstep 0 each vertex starts with its own label
        a0((1:1)) --- b0((2:2))
        b0 --- c0((3:3))
        d0((4:4)) --- e0((5:5))
    end
    subgraph s1 Superstep 1 minimum label advances one hop
        a1((1:1)) --- b1((2:1))
        b1 --- c1((3:2))
        d1((4:4)) --- e1((5:4))
    end
    subgraph s2 Converged all vertices share the minimum label of their component
        a2((1:1)) --- b2((2:1))
        b2 --- c2((3:1))
        d2((4:4)) --- e2((5:4))
    end
"""

# ──────────────────────────────────────────────────────────────────────
# 6. Shortest Paths
# ──────────────────────────────────────────────────────────────────────
shortest_paths = """
graph LR
    A["A:0"] -->|"+1"| B["B:1"]
    A -->|"+1"| C["C:1"]
    B -->|"+1"| D["D:2"]
    C -->|"+1"| D
    D -->|"+1"| E["E:3"]
"""

# ──────────────────────────────────────────────────────────────────────
# 7. Reputation Propagation
# ──────────────────────────────────────────────────────────────────────
reputation_propagation = """
graph LR
    subgraph Users
        U1["User A - Rep=500"]
        U2["User B - Rep=1200"]
        U3["User C - Rep=50"]
    end

    subgraph Answers
        A1["Answer 1 - Score=5"]
        A2["Answer 2 - Score=2"]
        A3["Answer 3 - Score=8"]
    end

    subgraph Questions
        Q1["Question - authority=0"]
    end

    U1 -->|"Posts"| A1
    U2 -->|"Posts"| A2
    U3 -->|"Posts"| A3
    A1 -->|"Answers"| Q1
    A2 -->|"Answers"| Q1
    A3 -->|"Answers"| Q1

    subgraph Result
        Q2["Question - authority = 500 + 1200 + 50 = 1750"]
    end

    Q1 -.->|"Pregel aggregates"| Q2
"""

# ──────────────────────────────────────────────────────────────────────
# 8. Debug Trace
# ──────────────────────────────────────────────────────────────────────
debug_trace = """
graph LR
    A1["A: path=A"] -->|"send path"| B1["B: path=A,B"]
    A1 -->|"send path"| C1["C: path=A,C"]
    B1 -->|"send path"| C2["C: path=A,B,C"]
    C1 -->|"send path"| D2["D: path=A,C,D"]
"""

PREGEL_DIAGRAMS = {
    "pregel-bsp-model": bsp_model,
    "pregel-in-degree-am": in_degree_am,
    "pregel-in-degree-pregel": in_degree_pregel,
    "pregel-pagerank-iterations": pagerank_iterations,
    "pregel-connected-components": connected_components,
    "pregel-shortest-paths": shortest_paths,
    "pregel-reputation-propagation": reputation_propagation,
    "pregel-debug-trace": debug_trace,
}


# ══════════════════════════════════════════════════════════════════════════════
# MOTIF FINDING TUTORIAL DIAGRAMS  →  docs/src/img/motif-diagrams/
# ══════════════════════════════════════════════════════════════════════════════

# ──────────────────────────────────────────────────────────────────────
# 1. Directed Graphlet Overview — 2-node and 3-node patterns
#    Replaces 4-node-directed-graphlets.png
# ──────────────────────────────────────────────────────────────────────
directed_graphlets_overview = """
graph LR
    subgraph G1 single directed edge
        g1a((A)) --> g1b((B))
    end

    subgraph G2 mutual edges
        g2a((A)) --> g2b((B))
        g2b --> g2a
    end

    subgraph G3 directed path
        g3a((A)) --> g3b((B))
        g3b --> g3c((C))
    end

    subgraph G4 directed 3-cycle
        g4a((A)) --> g4b((B))
        g4b --> g4c((C))
        g4c --> g4a
    end

    subgraph G5 divergent triangle
        g5a((A)) --> g5b((B))
        g5a --> g5c((C))
        g5c --> g5b
    end

    subgraph G6 convergent triangle
        g6a((A)) --> g6b((B))
        g6a --> g6c((C))
        g6b --> g6c
    end

    subgraph G7 divergent fork
        g7a((A)) --> g7b((B))
        g7a --> g7c((C))
    end

    subgraph G8 convergent fork
        g8a((A)) --> g8c((C))
        g8b((B)) --> g8c
    end
"""

# ──────────────────────────────────────────────────────────────────────
# 2. G4 and G5 Triangles — side by side
#    Replaces G4_and_G5_directed_network_motif.png
# ──────────────────────────────────────────────────────────────────────
g4_g5_triangles = """
graph LR
    subgraph G4 Continuous Triangle
        g4a((a)) --> g4b((b))
        g4b --> g4c((c))
        g4c --> g4a
    end

    subgraph G5 Divergent Triangle
        g5a((a)) --> g5b((b))
        g5a --> g5c((c))
        g5c --> g5b
    end
"""

# ──────────────────────────────────────────────────────────────────────
# 3. G30 Opposed 3-path
#    Replaces Directed-Graphlet-G30.png
# ──────────────────────────────────────────────────────────────────────
g30_opposed_3path = """
graph LR
    a((a)) -->|e1| b((b))
    b -->|e2| c((c))
    d((d)) -->|e3| c
"""

# ──────────────────────────────────────────────────────────────────────
# 4. G4 concrete Stack Exchange example
#    Question link-cycle — the dominant G4 instance in the dataset
# ──────────────────────────────────────────────────────────────────────
g4_stackexchange = """
graph LR
    qa((Question A)) -->|Links| qb((Question B))
    qb -->|Links| qc((Question C))
    qc -->|Links| qa
"""

# ──────────────────────────────────────────────────────────────────────
# 5. G5 concrete Stack Exchange example
#    Top result: Tag applied to two linked Questions
# ──────────────────────────────────────────────────────────────────────
g5_stackexchange = """
graph LR
    subgraph top 1775 instances Tag-Question triangle
        tag([Tag]) -->|"Tags"| qb((Question B))
        tag -->|"Tags"| qc((Question C))
        qc -->|"Links"| qb
    end

    subgraph self 274 instances User answers own question
        user[User] -->|"Asks"| ques((Question))
        user -->|"Posts"| ans((Answer))
        ans -->|"Answers"| ques
    end
"""

# ──────────────────────────────────────────────────────────────────────
# 6. G30 concrete Stack Exchange example
#    Top correlated result: two Votes cast for a pair of linked Questions
# ──────────────────────────────────────────────────────────────────────
g30_stackexchange = """
graph LR
    va{Vote A} -->|"CastFor"| qb((Question B))
    qb -->|"Links"| qc((Question C))
    vd{Vote D} -->|"CastFor"| qc
"""

MOTIF_DIAGRAMS = {
    "motif-directed-graphlets-overview": (directed_graphlets_overview, 1100),
    "motif-g4-g5-triangles": (g4_g5_triangles, 900),
    "motif-g30-opposed-3path": (g30_opposed_3path, 500),
    "motif-g4-stackexchange": (g4_stackexchange, 700),
    "motif-g5-stackexchange": (g5_stackexchange, 900),
    "motif-g30-stackexchange": (g30_stackexchange, 700),
}


if __name__ == "__main__":
    pregel_dir = _IMG_ROOT / "pregel-diagrams"
    for name, code in PREGEL_DIAGRAMS.items():
        save_diagram(name, code, pregel_dir)

    motif_dir = _IMG_ROOT / "motif-diagrams"
    for name, (code, width) in MOTIF_DIAGRAMS.items():
        save_diagram(name, code, motif_dir, width=width)
