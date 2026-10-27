from pyspark.sql.utils import is_remote

from .aggregate_messages import AggregateMessages

if is_remote():
    from graphframes.connect.graphframes_client import PregelConnect as Pregel
else:
    from .pregel import Pregel

__all__ = ["AggregateMessages", "Pregel"]
