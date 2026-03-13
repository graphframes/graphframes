"""Generate Mermaid diagrams for the Pregel tutorial.

Usage:
    python python/graphframes/tutorials/generate_diagrams.py

Renders Mermaid diagram source to SVG using the mmdc package (PhantomJS-based).
"""

import os
from pathlib import Path

from mmdc import MermaidConverter

OUTPUT_DIR = "docs/src/img/pregel-diagrams"
os.makedirs(OUTPUT_DIR, exist_ok=True)

converter = MermaidConverter(timeout=30)


def save_diagram(name: str, mermaid_code: str) -> None:
    """Render a Mermaid diagram to SVG via mmdc."""
    svg_path = Path(OUTPUT_DIR) / f"{name}.svg"
    try:
        converter.convert(
            mermaid_code.strip(),
            output_file=svg_path,
            theme="default",
            background="white",
            width=900,
        )
        print(f"Saved: {svg_path}")
    except Exception as e:
        print(f"Error rendering {name}: {e}")
        # Save the mermaid code as .mmd file for manual rendering
        mmd_path = Path(OUTPUT_DIR) / f"{name}.mmd"
        mmd_path.write_text(mermaid_code.strip())
        print(f"Saved Mermaid source: {mmd_path}")


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
connected_components = """
graph LR
    A0["A:A"] --- B0["B:B"] --- C0["C:C"]
    D0["D:D"] --- E0["E:E"] --- F0["F:F"]

    A1["A:A"] --- B1["B:A"] --- C1["C:B"]
    D1["D:D"] --- E1["E:D"] --- F1["F:E"]

    A2["A:A"] --- B2["B:A"] --- C2["C:A"]
    D2["D:D"] --- E2["E:D"] --- F2["F:D"]
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

if __name__ == "__main__":
    diagrams = {
        "pregel-bsp-model": bsp_model,
        "pregel-in-degree-am": in_degree_am,
        "pregel-in-degree-pregel": in_degree_pregel,
        "pregel-pagerank-iterations": pagerank_iterations,
        "pregel-connected-components": connected_components,
        "pregel-shortest-paths": shortest_paths,
        "pregel-reputation-propagation": reputation_propagation,
        "pregel-debug-trace": debug_trace,
    }

    for name, code in diagrams.items():
        save_diagram(name, code)
