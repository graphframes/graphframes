from .aggregate_messages import AggregateMessages

try:
    from .pregel import Pregel
except ImportError:
    # pyspark-client (pure Spark Connect) does not ship py4j / classic modules,
    # so the JVM-backed Pregel cannot be imported there.
    from graphframes.connect.graphframes_client import PregelConnect as Pregel

__all__ = ["AggregateMessages", "Pregel"]
