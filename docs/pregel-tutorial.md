---
layout: global
displayTitle: GraphFrames Pregel API Tutorial
title: Pregel API Tutorial
description: GraphFrames GRAPHFRAMES_VERSION Pregel API Tutorial - HOWTO scale up slow algorithms
---

This tutorial covers GraphFrames' aggregateMessages API for developing graph algorithms using [Pregel](https://15799.courses.cs.cmu.edu/fall2013/static/papers/p135-malewicz.pdf), a [bulk synchronous parallel](https://en.wikipedia.org/wiki/Bulk_synchronous_parallel) algorithm for distributed graph processing. It teaches you how to write highly scalabe graph algorithms using Pregel.

* Table of contents (This text will be scraped.)
  {:toc}

<h1 id="pregel">What is Pregel?</h1>

Pregel is a [bulk synchronous parallel](https://en.wikipedia.org/wiki/Bulk_synchronous_parallel) algorithm for large scale graph processing described in the landmark 2010 paper [Pregel: A System for Large-Scale Graph Processing](https://15799.courses.cs.cmu.edu/fall2013/static/papers/p135-malewicz.pdf) from Grzegorz Malewicz, Matthew H. Austern, Aart J. C. Bik, James C. Dehnert, Ilan Horn, Naty Leiser, and Grzegorz Czajkowski at Google.

<h1 id="stackexchange">Tutorial Dataset</h1>

As in the [Network Motif Tutorial](motif-tutorial.html#download-the-stack-exchange-dump-for-statsmeta), we will work with the [Stack Exchange Data Dump hosted at the Internet Archive](https://archive.org/details/stackexchange) using PySpark to build a property graph. To generate the knowledge graph for this tutorial, please refer to the [motif finding tutorial](motif-tutorial.html#download-the-stack-exchange-dump-for-statsmeta) before moving on to the next section.

<h1 id="inDegree">In-Degree in Pregel with aggreagateMessages</h1>

We begin with the simplest algorithm Pregel can run: computing the in-degree of every node in the graph.

First let's load our stats.meta knowledge graph and setup a SparkSession:

<div data-lang="python" markdown="1">
{% highlight python %}
import pyspark.sql.functions as F
from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession

# Initialize a SparkSession
spark: SparkSession = (
    SparkSession.builder.appName("Stack Overflow Motif Analysis")
    # Lets the Id:(Stack Overflow int) and id:(GraphFrames ULID) coexist
    .config("spark.sql.caseSensitive", True)
    .getOrCreate()
)
sc: SparkContext = spark.sparkContext
sc.setCheckpointDir("/tmp/graphframes-checkpoints")

# Change me if you download a different stackexchange site
STACKEXCHANGE_SITE = "stats.meta.stackexchange.com"
BASE_PATH = f"python/graphframes/tutorials/data/{STACKEXCHANGE_SITE}"

# We created these in stackexchange.py from Stack Exchange data dump XML files
NODES_PATH: str = f"{BASE_PATH}/Nodes.parquet"
nodes_df: DataFrame = spark.read.parquet(NODES_PATH)

# We created these in stackexchange.py from Stack Exchange data dump XML files
EDGES_PATH: str = f"{BASE_PATH}/Edges.parquet"
edges_df: DataFrame = spark.read.parquet(EDGES_PATH)


{% endhighlight %}
</div>

Now let's walk through in-degree in Pregel:

<div data-lang="python" markdown="1">
{% highlight python %}
# Initialize a column with 1 to transmit to other nodes
nodes_df = nodes_df.withColumn("start_degree", F.lit(1))

# Create a GraphFrame to get access to the Pregel aggregateMessages API
g: GraphFrame = GraphFrame(nodes_df, edges_df)

msgToDst = AM.src["start_degree"]
agg = g.aggregateMessages(
    F.sum(AM.msg).alias("in_degree"),
    sendToDst=msgToDst)
agg.show()
{% endhighlight %}
</div>

We now join the Pregel degrees with the normal `g.inDegree` API to verify all values are identical:

<div data-lang="python" markdown="1">
{% highlight python %}
# Join the Pregel degree with the normal GraphFrame.inDegree API
agg.join(g.inDegrees, on="id").show()
{% endhighlight %}
</div>

They are, as you can see below :)

<div data-lang="python" markdown="1">
{% highlight python %}
+------------------------------------+---------+--------+
|id                                  |in_degree|inDegree|
+------------------------------------+---------+--------+
|10719232-7477-4189-9695-4f08b7a89853|27       |27      |
|470b6c69-41b3-4f08-b01c-9503b8face38|11       |11      |
|757efc82-5197-4d70-8df6-c887a636c1c8|17       |17      |
|0d07e249-d46d-421b-9de9-64fe388ba9ef|8        |8       |
|8ab3818a-f8a4-4cf7-91d6-e049e54342ce|6        |6       |
|51263d00-e0d0-429f-ad62-66cb9ee6b236|22       |22      |
|bb13c447-4c53-4679-abf8-62e894c3f063|3        |3       |
|8843ef7d-4fb6-4eb9-ad73-54c76083c955|10       |10      |
|1fb4aa84-bcdc-4ae2-b4c0-bfa715f87603|2        |2       |
|91f9eb5e-41f3-4d1b-9032-0554f0223bb9|7        |7       |
+------------------------------------+---------+--------+
{% endhighlight %}
</div>

<h1 id="pagerank">Implementing PageRank with aggregateMesssages</h1>

Let's move on to something more complex. PageRank was defined by Google cofounders Larry Page and Sergey Brin in a landmark 1999 paper <a href="https://www.cis.upenn.edu/~mkearns/teaching/NetworkedLife/pagerank.pdf">The PageRank Citation Rakning: Bringing Order to the Web</a>.

<center>
    <figure>
        <img src="img/Simplified-PageRank-Calculation.jpg" width="800px" />
        <figcaption>A Simplified PageRank Calculation, from the <a href="https://www.cis.upenn.edu/~mkearns/teaching/NetworkedLife/pagerank.pdf">PageRank paper</a></figcaption>
    </figure>
</center>

<div data-lang="python" markdown="1">
{% highlight python %}
# Initialize a column with 1 to transmit to other nodes
nodes_df = nodes_df.withColumn("start_pagerank", F.lit(1.0))

# Create a GraphFrame to get access to the Pregel aggregateMessages API
g: GraphFrame = GraphFrame(nodes_df, edges_df)

msgToDst = AM.src["start_degree"] / 
agg = g.aggregateMessages(
    F.sum(AM.msg).alias("in_degree"),
    sendToDst=msgToDst)
agg.show()
{% endhighlight %}
</div>


<center>
    <figure>
        <img src="" width="800px" alt="" title="" style="margin: 10px 25px 10px 25px" />
        <figcaption><a href=""></a></figcaption>
    </figure>
</center>

<div data-lang="python" markdown="1">
{% highlight python %}

{% endhighlight %}
</div>

<h2 id="combine-node-types">Combining Node Types</h2>

<h1 id="conclusion">Conclusion</h1>

In this tutorial, we learned to use GraphFrames' Pregel API.
