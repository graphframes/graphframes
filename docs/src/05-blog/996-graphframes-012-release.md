# GraphFrames 0.12.0 release

- **Published:** 2026-06-12T00:00:00Z
- **Title:** GraphFrames 0.12.0 release
- **Summary:** This release brings new Community Detection algorithm, new API to find all simple paths between subset of vertices and significant performance improvements for the Two-Phase Connected Components Algorithm.

## New Contributors

- [@slavlotski](https://github.com/slavlotski) -- `asReversed` helper API to reverse all the edges of the graph

## New Community Detection Algorithm

Previous versions of GraphFrames relied entirely on the most naive implementation of the Label Propagation algorithm. While this implementation is fast and well-known, the quality of the output clusters is questionable, and the algorithm itself is unstable. Even small changes in the local structure can alter the output.

The new algorithm significantly modifies the original Label Propagation algorithm. While it follows the same idea that allows for efficient implementation on distributed graphs, it also provides more flexibility. The inspiration came from [Xie, Jierui, and Boleslaw K. Szymanski. "Community detection using a neighborhood strength driven label propagation algorithm." 2011 IEEE Network Science Workshop. IEEE, 2011.](https://arxiv.org/abs/1105.3264)

The core idea is that, during propagation, vertices choose a community based not only on their local neighborhood, but also on the number of neighbors they have in common with other community members. Compared to existing label propagation, the new algorithm also supports passing initial labels, which allows it to be used incrementally or for semi-supervised community detection.

Credits to [@SemyonSinchenko](https://github.com/SemyonSinchenko).

## New all paths API

After introducing the `AggregateNeighbors` API in version `0.11.0`, which is a generic, multi-hop aggregation API, GraphFrames is receiving built-in implementations based on neighbor aggregation. The first is the long-awaited API that finds all simple paths between a subset of vertices.

Credits to [@SemyonSinchenko](https://github.com/SemyonSinchenko).

## Performance optimizations in Connected Components

The Two-Phase algorithm is based on the idea of rewiring edges to end up with a star-like graph structure. However, during the rewiring process, a large number of leaves, or vertices with no outgoing edges, appear. Although determining components for these vertices is trivial, and they do not participate in the main algorithm loop, they still shuffle and join until full convergence. The new optimization adds an efficient way to determine the optimal time to remove such leaves and offset the cost of rejoining them after convergence. Based on initial benchmarks, the optimization delivers a ~25% performance boost.

Credits to [WeichenXu123](https://github.com/WeichenXu123)

## Future steps

- Moving in the direction of support of full-featured graph queries
- Improving GraphFrames capabilities in Graph ML
- Adding features useful in Spatial Graphs analysis

