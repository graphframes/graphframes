"""Neo4j integration for GraphFrames tutorials.

This package provides CLI commands for:
- Starting/stopping Neo4j Docker containers with APOC
- Loading Stack Exchange Parquet data into Neo4j
- Checking Neo4j status and statistics
"""

from graphframes.tutorials.neo4j.cli import neo4j

__all__ = ["neo4j"]
