# Installation

If you are new to using Apache Spark, refer to the [Apache Spark Documentation](http://spark.apache.org/docs/latest/index.html) and its [Quick-Start Guide](http://spark.apache.org/docs/latest/quick-start.html) for more information.

## Spark Versions Compatibility

| Component           | Spark 3.x (Scala 2.12) | Spark 3.x (Scala 2.13) | Spark 4.x (Scala 2.13) |
|---------------------|------------------------|------------------------|------------------------|
| graphframes         | ✓                      | ✓                      | ✓                      |
| graphframes-connect | ✓                      | ✓                      | ✓                      |

The following example shows how to run the Spark shell with the GraphFrames package. We use the `--packages` argument to download the graphframes package and any dependencies automatically.

## Spark 3.x

### Spark Shell

```shell
$ ./bin/spark-shell --packages io.graphframes:graphframes-spark3_2.12:@VERSION@
```

Or use the following command to force using of Scala 2.13:

```shell
$ ./bin/spark-shell --packages io.graphframes:graphframes-spark3_2.13:@VERSION@
```
### PySpark

```shell
$ pip install graphframes-py==@VERSION@
$ ./bin/pyspark --packages io.graphframes:graphframes-spark3_2.12:@VERSION@
```

## Spark 4.x

### Spark Shell

```shell
$ ./bin/spark-shell --packages io.graphframes:graphframes-spark4_2.13:@VERSION@
```

### PySpark

```shell
$ pip install graphframes-py==@VERSION@
$ ./bin/pyspark --packages io.graphframes:graphframes-spark4_2.13:@VERSION@
```

## Spark Connect Server Extension

To add GraphFrames to your spark connect server, you need to specify the plugin name:

For Spark 4.x:

```shell
./sbin/start-connect-server.sh \
  --conf spark.connect.extensions.relation.classes=\
  org.apache.spark.sql.graphframes.GraphFramesConnect \
  --packages io.graphframes.graphframes-connect-spark4_2.13:@VERSION@
```

For Spark 3.x:

```shell
./sbin/start-connect-server.sh \
  --conf spark.connect.extensions.relation.classes=\
  org.apache.spark.sql.graphframes.GraphFramesConnect \
  --packages io.graphframes.graphframes-connect-spark3_2.12:@VERSION@
```

**WARNING**: The GraphFrames Connect Server Extension is not compatible with managed SparkConnect from Databricks. To make it work, you need to use build GraphFrames Connect Server Extension from source with a flag:

```shell
./build/sbt -Dvendor.name=dbx connect/assembly
```

### Spark Connect Clients

At the moment GraphFrames has only PySpark client bundled with the package: `pip install graphframes-py==@VERSION@`. In Runtime GraphFrames PySpark client will automatically handle the connection to the GraphFrames Connect Server Extension in case it is Spark Connect environment.

### Messages

At the moment, the following APIs are exposed:

```protobuf
message GraphFramesAPI {
  bytes vertices = 1;
  bytes edges = 2;
  oneof method {
    AggregateMessages aggregate_messages = 3;
    BFS bfs = 4;
    ConnectedComponents connected_components = 5;
    DropIsolatedVertices drop_isolated_vertices = 6;
    FilterEdges filter_edges = 7;
    FilterVertices filter_vertices = 8;
    Find find = 9;
    LabelPropagation label_propagation = 10;
    PageRank page_rank = 11;
    ParallelPersonalizedPageRank parallel_personalized_page_rank = 12;
    PowerIterationClustering power_iteration_clustering = 13;
    Pregel pregel = 14;
    ShortestPaths shortest_paths = 15;
    StronglyConnectedComponents strongly_connected_components = 16;
    SVDPlusPlus svd_plus_plus = 17;
    TriangleCount triangle_count = 18;
    Triplets triplets = 19;
  }
}
```

## Building GraphFrames from Source

```shell
./build/sbt package
```

## Nightly Builds

GraphFrames project is publishing SNAPSHOTS (nightly builds) to the "Central Portal Snapshots." Please read [this section](https://central.sonatype.org/publish/publish-portal-snapshots/#consuming-snapshot-releases-for-your-project) of the Sonatype documentation to check how can you use snapshots in your project.

GroupId: `io.graphframes`
ArtifactIds:

* `graphframes-spark3_2.12`
* `graphframes-spark3_2.13`
* `graphframes-connect-spark3_2.12`
* `graphframes-connect-spark3_2.13`
* `graphframes-spark4_2.13`
* `graphframes-connect-spark4_2.13`

