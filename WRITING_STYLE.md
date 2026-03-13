# Russell Jurney Writing Style Guide

This document captures the writing style of Russell Jurney, derived from analysis of his published technical tutorials, blog posts, and conference material. The Pregel tutorial should emulate this style.

## Voice and Tone

### Authoritative but Approachable
Russell writes with the confidence of someone who has shipped production graph systems at scale (Walmart, LinkedIn-adjacent work, Apache committer). He doesn't hedge unnecessarily, but he's honest about trade-offs and limitations. He speaks directly to the reader as a peer, not down to them.

### First-Person, Conversational
He uses "I," "we," and "you" naturally. "We're going to build..." not "One might consider building..." He addresses the reader directly: "Keep in mind that I am talking about paths - through aggregation a motif might cover thousands of nodes!"

### Teaching Through Doing
His tutorials aren't API documentation with examples bolted on. They are *guided explorations* where the reader discovers insights alongside the author. He starts with real-world curiosity ("What patterns exist in this knowledge graph?") and lets the tools serve the investigation.

## Structural Patterns

### Progressive Complexity
Every tutorial starts simple and builds. In the motif tutorial: simple triangles → divergent triangles → 4-node paths → property graph motifs → correlation analysis. Each step introduces one new concept.

### Theory-Then-Practice, Interleaved
Academic papers are cited and explained *before* the code that implements them. But the explanation is brief - just enough to understand *why* the code works. Then the code runs, results appear, and discussion follows. This cycle repeats:

1. Motivate (why do we care?)
2. Explain (what's the theory?)
3. Implement (here's the code)
4. Observe (here's what happened)
5. Interpret (what does it mean?)

### Real-World Data, Not Toy Examples
Russell insists on real datasets. The motif tutorial uses actual Stack Exchange data - not a 4-node toy graph. This grounds the tutorial in practical reality and makes the results meaningful. Small test graphs are used *only* to verify understanding before scaling up.

### Complete, Runnable Code
Every code block is copy-paste runnable. No `...` ellipses hiding complexity. No "exercise left to the reader." The reader can follow along in a terminal and get the same results.

## Language Characteristics

### Short Paragraphs
Paragraphs are 2-4 sentences. Dense but not overwhelming. Each paragraph makes one point.

### Technical Precision with Plain English
He uses the correct technical terms (graphlets, motifs, property graphs, bulk synchronous parallel) but always explains them in plain language first. He defines terms inline rather than in a glossary.

### Practical Asides
"To let you know of a hard-won tip: alias the edge with its pattern." These practical tips, earned from experience, are scattered throughout. They build trust and add texture.

### No Filler
No "In this section, we will discuss..." preambles. No "As we mentioned earlier..." callbacks. He gets to the point immediately. If a section heading says "PageRank," the first sentence is about PageRank.

### Numbered Lists for Sequences
When explaining multi-step processes or ranked results, he uses numbered lists. For options or characteristics, bullet points.

## Visual and Formatting Patterns

### Inline Output
Code runs and output appears immediately below it, properly formatted. The reader sees exactly what they'd see in their terminal.

### Data Tables
Output tables from Spark `.show()` calls are included verbatim. He often adds comma formatting for readability: `.withColumn("count", F.format_number(F.col("count"), 0))`.

### Academic Citations
Papers are cited with full author lists and linked. He treats the tutorial as a bridge between academic research and practical engineering.

### Diagrams from Papers
Uses diagrams from the referenced academic papers (with proper attribution) to ground the technical discussion visually.

## Emotional Register

### Genuine Curiosity
"104 - we hit the network motif jackpot with this pattern!" He gets visibly excited when data reveals interesting patterns. This enthusiasm is contagious.

### Intellectual Honesty
"In this instance I will limit myself to a 4-path pattern as you may not have a Spark cluster on which to learn." He acknowledges limitations without apologizing.

### Professional Pride
His tutorials are thorough because his professional reputation depends on them. Every example works. Every explanation is accurate. He's a consultant and open-source maintainer - broken tutorials lose clients and community trust.

## Anti-Patterns (Things to Avoid)

- Don't write "In conclusion..." at the start of a conclusion section
- Don't use passive voice for code actions ("The data is loaded" → "We load the data")
- Don't explain what code will do before showing it - show it, then explain
- Don't use jargon without definition
- Don't show partial code blocks
- Don't skip error handling or edge cases
- Don't separate theory from practice by more than a few paragraphs
