"""Generate Mermaid diagrams for the Pregel and Motif Finding tutorials.

Usage:
    poetry install --with docs
    python python/graphframes/tutorials/generate_diagrams.py

Renders Mermaid diagram source to SVG using mermaidx, which bundles its own
JavaScript engine (quickjs) -- no browser or Node install required.
"""

import re
from pathlib import Path

import mermaidx

# Resolve repo root relative to this file so the script works regardless of cwd
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
_IMG_ROOT = _REPO_ROOT / "docs" / "src" / "img"

# Pixels to raise edge labels off the line they annotate (see save_diagram).
EDGE_LABEL_LIFT = 16


def save_diagram(
    name: str, mermaid_code: str, output_dir: Path, config: dict | None = None
) -> None:
    """Render a Mermaid diagram to SVG via mermaidx and tidy up the output.

    `config` is an optional Mermaid config dict (see DIAGRAM_CONFIG) for
    diagrams that need tighter-than-default spacing.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    svg_path = output_dir / f"{name}.svg"
    try:
        kwargs: dict = {"theme": "default"}
        if config:
            kwargs["config"] = config
        svg_text = mermaidx.render(mermaid_code.strip(), **kwargs).svg()
        # Post-process SVG:
        # 1. Add a white background rect so the diagram isn't transparent
        svg_text = re.sub(
            r"(<svg[^>]*>)",
            r'\1<rect width="100%" height="100%" fill="white"/>',
            svg_text,
        )
        # 2. Lift edge labels off the line they annotate. Mermaid centres each
        #    label on the edge midpoint, so it sits on top of the line (and
        #    sometimes on a subgraph border). Shifting the group up clears both.
        svg_text = re.sub(
            r'(<g class="edgeLabel" transform="translate\(\s*[-\d.]+\s*,\s*)([-\d.]+)(\s*\))',
            lambda m: f"{m.group(1)}{float(m.group(2)) - EDGE_LABEL_LIFT}{m.group(3)}",
            svg_text,
        )
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
# Panels are declared s2, s1, s0 because sibling subgraphs render in REVERSE
# declaration order -- declaring s0 first put "Converged" on the left, reading
# the algorithm backwards. Titles must be bracketed and quoted: with the bare
# form (`subgraph s0 Superstep 0 ...`) the id leaks into the rendered title,
# which is why the old figure read "s0 Superstep 0 each vertex...".
connected_components = """
graph TB
    subgraph s2["Converged: all vertices share their component minimum"]
        direction LR
        a2((1:1)) --- b2((2:1))
        b2 --- c2((3:1))
        d2((4:4)) --- e2((5:4))
    end
    subgraph s1["Superstep 1: minimum label advances one hop"]
        direction LR
        a1((1:1)) --- b1((2:1))
        b1 --- c1((3:2))
        d1((4:4)) --- e1((5:4))
    end
    subgraph s0["Superstep 0: each vertex starts with its own label"]
        direction LR
        a0((1:1)) --- b0((2:2))
        b0 --- c0((3:3))
        d0((4:4)) --- e0((5:5))
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
# Laid out as 2 rows of 4. Three Mermaid behaviours are load-bearing:
#   1. Under `graph TB`, sibling subgraphs lay out HORIZONTALLY (the opposite
#      of what the direction reads); `graph LR` would stack them into one
#      2500px-tall column.
#   2. Siblings render in REVERSE declaration order, so each row is declared
#      right-to-left and the second row is declared before the first.
#   3. The invisible links (`~~~`) at the bottom drop G5-G8 onto a second rank.
#      Wrapping each row in its own subgraph also works, but the wrapper
#      reserves title space and leaves a large gap between the rows.
# Rendered with the tighter spacing in DIAGRAM_CONFIG.
directed_graphlets_overview = """
graph TB
    subgraph G4["G4 directed 3-cycle"]
        direction LR
        g4a((A)) --> g4b((B))
        g4b --> g4c((C))
        g4c --> g4a
    end
    subgraph G3["G3 directed path"]
        direction LR
        g3a((A)) --> g3b((B))
        g3b --> g3c((C))
    end
    subgraph G2["G2 mutual edges"]
        direction LR
        g2a((A)) --> g2b((B))
        g2b --> g2a
    end
    subgraph G1["G1 single directed edge"]
        direction LR
        g1a((A)) --> g1b((B))
    end
    subgraph G8["G8 convergent fork"]
        direction LR
        g8a((A)) --> g8c((C))
        g8b((B)) --> g8c
    end
    subgraph G7["G7 divergent fork"]
        direction LR
        g7a((A)) --> g7b((B))
        g7a --> g7c((C))
    end
    subgraph G6["G6 convergent triangle"]
        direction LR
        g6a((A)) --> g6b((B))
        g6a --> g6c((C))
        g6b --> g6c
    end
    subgraph G5["G5 divergent triangle"]
        direction LR
        g5a((A)) --> g5b((B))
        g5a --> g5c((C))
        g5c --> g5b
    end
    g1a ~~~ g5a
    g2a ~~~ g6a
    g3a ~~~ g7a
    g4a ~~~ g8a
"""

# ──────────────────────────────────────────────────────────────────────
# 2. G4 and G5 Triangles — side by side
#    Replaces G4_and_G5_directed_network_motif.png
# ──────────────────────────────────────────────────────────────────────
# `graph TB` puts the two panels side by side; declared in reverse so G4 lands
# on the left. See directed_graphlets_overview above for the full explanation.
g4_g5_triangles = """
graph TB
    subgraph G5["G5 Divergent Triangle"]
        direction LR
        g5a((a)) --> g5b((b))
        g5a --> g5c((c))
        g5c --> g5b
    end

    subgraph G4["G4 Continuous Triangle"]
        direction LR
        g4a((a)) --> g4b((b))
        g4b --> g4c((c))
        g4c --> g4a
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
# Three Mermaid gotchas are load-bearing here:
#   1. Top-level `graph TB` is what makes the two unconnected subgraphs sit
#      SIDE BY SIDE; `graph LR` stacks them into a ~400x1060 column instead.
#      (Same trick as connected_components above.)
#   2. `direction LR` inside each subgraph keeps the motif itself reading
#      left-to-right within its panel.
#   3. Subgraphs are laid out right-to-left, so the headline (1,775) case is
#      declared SECOND in order to appear on the left, where it reads first.
# Bracketed subgraph titles are used so the labels can contain punctuation.
# ──────────────────────────────────────────────────────────────────────
g5_stackexchange = """
graph TB
    subgraph self["274 instances: User answers their own Question"]
        direction LR
        user[User] -->|"Asks"| ques((Question))
        user -->|"Posts"| ans((Answer))
        ans -->|"Answers"| ques
    end

    subgraph top["1,775 instances: Tag applied to two linked Questions"]
        direction LR
        tag([Tag]) -->|"Tags"| qb((Question B))
        tag -->|"Tags"| qc((Question C))
        qc -->|"Links"| qb
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
    "motif-directed-graphlets-overview": directed_graphlets_overview,
    "motif-g4-g5-triangles": g4_g5_triangles,
    "motif-g30-opposed-3path": g30_opposed_3path,
    "motif-g4-stackexchange": g4_stackexchange,
    "motif-g5-stackexchange": g5_stackexchange,
    "motif-g30-stackexchange": g30_stackexchange,
}


# Per-diagram Mermaid config, for diagrams that need tighter-than-default
# spacing. nodeSpacing drives BOTH the gap between rows and the separation of
# nodes inside each panel, so it cannot go much below 14 without the fork
# patterns collapsing and the subgraph titles colliding with the graph content.
DIAGRAM_CONFIG = {
    "motif-directed-graphlets-overview": {
        "flowchart": {
            "diagramPadding": 2,
            "padding": 8,
            "nodeSpacing": 14,
            "rankSpacing": 14,
            "subGraphTitleMargin": {"top": 0, "bottom": 4},
        }
    },
    "pregel-connected-components": {
        "flowchart": {
            "diagramPadding": 2,
            "padding": 8,
            "nodeSpacing": 20,
            "rankSpacing": 24,
            "subGraphTitleMargin": {"top": 0, "bottom": 6},
        }
    },
}


if __name__ == "__main__":
    pregel_dir = _IMG_ROOT / "pregel-diagrams"
    for name, code in PREGEL_DIAGRAMS.items():
        save_diagram(name, code, pregel_dir, config=DIAGRAM_CONFIG.get(name))

    motif_dir = _IMG_ROOT / "motif-diagrams"
    for name, code in MOTIF_DIAGRAMS.items():
        save_diagram(name, code, motif_dir, config=DIAGRAM_CONFIG.get(name))
