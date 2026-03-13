# Pregel Tutorial Research

## The Pregel Programming Model

### Original Paper: Malewicz et al. (2010)
**"Pregel: A System for Large-Scale Graph Processing"**
- Published at SIGMOD 2010 by Google engineers
- Core abstraction: computation as a sequence of **supersteps**
- Each vertex executes a user-defined function in parallel
- Vertices communicate by sending messages along edges
- A **barrier** synchronizes between supersteps
- Vertices can **vote to halt** (become inactive) and are reactivated by incoming messages

### Bulk Synchronous Parallel (BSP) Model
Pregel implements the BSP model (Valiant, 1990):
1. **Compute**: Each vertex processes incoming messages and updates its state
2. **Communicate**: Each vertex sends messages to neighbors
3. **Barrier**: All processors synchronize before the next superstep

Key insight: BSP avoids the complexity of asynchronous distributed systems by enforcing synchronization barriers. This makes algorithms easier to reason about and debug.

### "Think Like a Vertex"
The paradigm shift in Pregel is **vertex-centric programming**:
- You write a function that executes at a single vertex
- The function sees only: the vertex's state, incoming messages, and outgoing edges
- The function can: update vertex state, send messages, vote to halt
- The system handles distribution, fault tolerance, and synchronization

This is fundamentally different from:
- **MapReduce**: Data-parallel, no graph structure awareness
- **Graph databases**: Query-centric, not computation-centric
- **Matrix operations**: Global view, not local vertex view

## GraphFrames' Pregel Implementation

### DataFrame-Based Design
Unlike GraphX (RDD-based), GraphFrames Pregel is built on DataFrames:
- Leverages Catalyst query optimizer
- Supports SQL-like column expressions for messages and updates
- Integrates with the broader Spark SQL ecosystem
- Better memory management through columnar storage

### API Design Pattern (Builder Pattern)
```python
result = graph.pregel \
    .setMaxIter(n) \
    .withVertexColumn("col", init_expr, update_expr) \
    .sendMsgToDst(msg_expr) \
    .aggMsgs(agg_expr) \
    .run()
```

### Key API Methods

1. **`withVertexColumn(name, init, update)`**: Define vertex state
   - `init`: Expression evaluated once at start (can reference original vertex columns)
   - `update`: Expression evaluated each superstep (can reference `Pregel.msg()`)

2. **`sendMsgToDst(expr)` / `sendMsgToSrc(expr)`**: Define messages
   - Can reference `Pregel.src(col)`, `Pregel.dst(col)`, `Pregel.edge(col)`
   - `null` messages are automatically filtered out

3. **`aggMsgs(expr)`**: Aggregate messages per target vertex
   - Standard aggregation functions: `sum`, `min`, `max`, `collect_list`, etc.

4. **Termination controls**:
   - `setMaxIter(n)`: Maximum iterations
   - `setEarlyStopping(True)`: Stop when no non-null messages
   - `setStopIfAllNonActiveVertices(True)`: Stop when all vertices vote to halt
   - `setInitialActiveVertexExpression(expr)`: Initial activity state
   - `setUpdateActiveVertexExpression(expr)`: Update activity after each superstep

5. **Memory optimization**:
   - `required_src_columns(cols)`: Only include needed source columns in triplets
   - `required_dst_columns(cols)`: Only include needed destination columns in triplets
   - Automatic optimization: skips dst join when dst columns not referenced in messages

### Internal Mechanics
From reading `Pregel.scala`:

1. Initialize vertices with vertex columns and active flag
2. For each iteration:
   a. Build triplets (src vertex + edge + dst vertex)
   b. Generate messages from triplets using `sendMsg*` expressions
   c. Explode and filter null messages
   d. Group by target vertex ID and aggregate
   e. Left-outer join aggregated messages with current vertices
   f. Update vertex columns using update expressions
   g. Checkpoint periodically (every 2 iterations by default)
   h. Check termination conditions

### Automatic Optimizations (Recent)
- **Skip second join**: When message expressions don't reference `dst.*` columns (other than `dst.id`), the dst vertex join is skipped entirely
- **Conditional partitioning**: Source-only vs source+destination repartitioning
- **Active vertex filtering**: Skip message generation from inactive vertices when `setSkipMessagesFromNonActiveVertices(True)`

## When Pregel Beats Other Approaches

### vs. Graph Databases (Neo4j, etc.)
- **Iterative convergence**: Pregel naturally expresses algorithms that converge over many iterations (PageRank, label propagation). Graph DBs require external loops.
- **Scalability**: Pregel on Spark scales to billions of edges. Graph DBs are typically single-machine.
- **Custom aggregation**: Arbitrary message/aggregation functions, not limited to Cypher.

### vs. GraphFrames' Built-in Algorithms
- **Custom algorithms**: When the built-in pageRank, shortestPaths, connectedComponents aren't enough.
- **Domain-specific computations**: Reputation propagation, influence scoring, custom centrality measures.
- **Heterogeneous graph algorithms**: Algorithms that vary behavior by node/edge type.

### vs. Simple DataFrame Operations
- **Multi-hop propagation**: Information that flows more than one hop requires iterative computation.
- **Convergence-based algorithms**: When you don't know how many iterations are needed.
- **State accumulation**: When vertex state evolves based on neighbor states over time.

### Unsuitable for Pregel
- **Single-pass aggregations**: Use AggregateMessages instead (simpler, faster).
- **Global computations**: Pregel is vertex-local; global statistics require separate computation.
- **Non-graph problems**: If data doesn't have graph structure, use regular Spark.

## Stack Exchange Dataset Analysis

### Graph Structure
From the motif tutorial analysis:
- **129,751 nodes** across 7 types: Badge (43K), Vote (42K), User (37K), Answer (3K), Question (2K), PostLinks (1.3K), Tag (143)
- **97,104 edges** across 8 types: Earns (43K), CastFor (40K), Tags (4.4K), Answers (3K), Posts (2.8K), Asks (1.9K), Links (1.2K), Duplicates (88)

### Creative Pregel Algorithm Ideas for Stack Exchange

1. **In-Degree Centrality**: Simplest possible Pregel - counting incoming edges
2. **PageRank**: Identify most important nodes in the knowledge graph
3. **Connected Components**: Find disconnected subgraphs in the network
4. **Shortest Paths**: Distance between landmark users/questions
5. **Reputation Propagation**: Custom algorithm - propagate user reputation through the answer graph
   - Users have Reputation scores
   - Answers connect Users to Questions
   - Questions accumulate "answer quality" from answerer reputation
   - This is genuinely hard to compute without Pregel
6. **Influence Tracing (Debug)**: Track which vertices influence which others through message paths
7. **Community Authority**: Combine tag co-occurrence with user activity to find topic authorities

## Academic References

1. Malewicz, G., et al. "Pregel: A System for Large-Scale Graph Processing." SIGMOD 2010.
2. Page, L., et al. "The PageRank Citation Ranking: Bringing Order to the Web." 1999.
3. Dave, A., et al. "GraphFrames: An Integrated API for Mixing Graph and Relational Queries." GRADES 2016.
4. Valiant, L. "A Bridging Model for Parallel Computation." CACM 1990.
5. Dean, J. and Ghemawat, S. "MapReduce: Simplified Data Processing on Large Clusters." OSDI 2004.
6. Zadeh, R. "CME 323: Distributed Algorithms and Optimization." Stanford, Spring 2015.

