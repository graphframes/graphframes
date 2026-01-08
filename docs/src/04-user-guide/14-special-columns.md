# Reserved Columns

GraphFrames internally use the following reserved columns:

- `id` for vertex IDs
- `src` for edge source IDs
- `dst` for edge destination IDs
- `attr` for vertex attributes during the GraphX conversion
- `new_id` for indexed Long IDs for vertices
- `new_src` for indexed Long IDs for edge sources
- `new_dst` for indexed Long IDs for edge destinations
- `graphx_attr` for vertex attributes during the GraphX conversion
- `weight` for edge weights
- `MSG` for messages in AggregateMessages
- `_pregel_msg` for pregel messages
- `_pregel_is_active` for pregel vertex active status

## Algorithm Specific Columns

- `component` for result of connected components
- `label` for result of label propagation
- `distances` for result of shortest paths
- `pagerank` for result of pagerank
- `count` for result of triangle count
- `column{1-4}` for SVD++ reserved columns
- `outDegree` for result of out degree
- `inDegree` for result of in degree
- `degree` for result of degree