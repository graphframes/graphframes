---
layout: global
displayTitle: GraphFrames Network Motif Finding Tutorial
title: Network Motif Finding Tutorial
description: GraphFrames GRAPHFRAMES_VERSION motif finding tutorial - teaches you to find motifs 
  using Stack Exchange data
---

This tutorial covers GraphFrames' motif finding feature. We perform pattern matching on a property graph representing a Stack Exchange site using Apache Spark and [GraphFrames' motif finding](user-guide.html#motif-finding) feature. We will download the `stats.meta` archive from the [Stack Exchange Data Dump at the Internet Archive](https://archive.org/details/stackexchange), use PySpark to build a property graph and then mine it for property graph network motifs by combining both graph and relational queries.

* Table of contents (This text will be scraped.)
  {:toc}

# Download the Stack Exchange Dump for [stats.meta.stackexchange.com](stats.meta.stackexchange.com) at Internet Archive

The Python examples include a CLI utility at `python/graphframes/examples/download.py` for downloading any site's [Stack Exchange Data Dump](https://archive.org/details/stackexchange) from the Internet Archive. The script takes the subdomain as an argument, downloads the corresponding 7zip archive and expands it into the `python/graphframes/examples/data` folder.

<div data-lang="bash" markdown="1">
{% highlight bash %}
Usage: download.py [OPTIONS] SUBDOMAIN

  Download Stack Exchange archive for a given SUBDOMAIN.

  Example: python/graphframes/examples/download.py stats.meta

  Note: This won't work for stackoverflow.com archives due to size.

Options:
  --data-dir TEXT           Directory to store downloaded files
  --extract / --no-extract  Whether to extract the archive after download
  --help                    Show this message and exit.
{% endhighlight %}
</div>

Use `download.py` to download the Stack Exchange Data Dump for `stats.meta.stackexchange.com`.

<div data-lang="bash" markdown="1">
{% highlight bash %}
$ python python/graphframes/examples/download.py stats.meta

Downloading archive from <https://archive.org/download/stackexchange/stats.meta.stackexchange.com.7z>
Downloading  [####################################]  100%
Download complete: python/graphframes/examples/data/stats.meta.stackexchange.com.7z
Extracting archive...
Extraction complete: stats.meta.stackexchange.com
{% endhighlight %}
</div>

# Build the Graph

We will build a property graph from the Stack Exchange data dump using PySpark in the [python/graphframes/examples/stackexchange.py](python/graphframes/examples/stackexchange.py) script. The data comes as a single XML file, so we use [spark-xml](https://github.com/databricks/spark-xml) (moving inside Spark as of 4.0) to load the data, extract the relevant fields and build the nodes and edges of the graph. For some reason Spark XML uses a lot of RAM, so we need to increase the driver and executor memory to at least 4GB.

<div data-lang="bash" markdown="1">
{% highlight bash %}
$ spark-submit --packages com.databricks:spark-xml_2.12:0.18.0 --driver-memory 4g --executor-memory 4g python/graphframes/examples/stackexchange.py
{% endhighlight %}
</div>

The script will output the nodes and edges of the graph in the `python/graphframes/examples/data` folder. We can now use GraphFrames to load the graph and perform motif finding.

# Motif Finding

We will use GraphFrames to find motifs in the Stack Exchange property graph. The script [python/graphframes/examples/motif.py](python/graphframes/examples/motif.py) demonstrates how to load the graph, define various motifs and find all instances of the motif in the graph.

NOTE: I use the terms `node` as interchangaable with `vertex` and `edge` with `link` or `relationship`. The API is [GraphFrame.vertices](https://graphframes.github.io/graphframes/docs/_site/api/python/graphframes.html#graphframes.GraphFrame.vertices) and [GraphFrames.edges](https://graphframes.github.io/graphframes/docs/_site/api/python/graphframes.html#graphframes.GraphFrame.edges) but some documentation says `relationships`. We need to add an alias from `g.vertices` to `g.nodes` and `g.edges` to both `g.relationships` and `g.links`.

For a quick run-through of the script, use the following command:

<div data-lang="bash" markdown="1">
{% highlight bash %}
spark-submit --packages com.databricks:spark-xml_2.12:0.18.0 python/graphframes/examples/stackexchange.py
{% endhighlight %}
</div>

Let's walk through what it does, line-by-line. The script starts by importing the necessary modules and defining some utility functions for visualizing paths returned by [g.find()](https://graphframes.github.io/graphframes/docs/_site/user-guide.html#motif-finding). Note we do need a `SparkContext` to set a checkpoint directory for [g.connectedComponents](https://graphframes.github.io/graphframes/docs/_site/user-guide.html#connected-components). Note that if you give `python/graphframes/examples/download.py` CLI a different subdomain, you will need to change the `STACKEXCHANGE_SITE` variable.

<div data-lang="python" markdown="1">
{% highlight python %}
import pyspark.sql.functions as F
from graphframes import GraphFrame
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from utils import three_edge_count, four_edge_count, add_degree, add_type_degree


#
# Initialize a SparkSession. You can configre SparkSession via: .config("spark.some.config.option", "some-value")
#
spark: SparkSession = (
    SparkSession.builder.appName("Stack Overflow Motif Analysis")
    # Lets the Id:(Stack Overflow int) and id:(GraphFrames ULID) coexist
    .config("spark.sql.caseSensitive", True)
    # Single node mode - 128GB machine
    # .config("spark.driver.memory", "4g")
    # .config("spark.executor.memory", "4g")
    .getOrCreate()
)
sc: SparkContext = spark.sparkContext

# Change me if you download a different stackexchange site
STACKEXCHANGE_SITE = "stats.meta.stackexchange.com"
BASE_PATH = f"python/graphframes/examples/data/{STACKEXCHANGE_SITE}"
{% endhighlight %}
</div>

Next, we load the nodes and edges of the graph from the `data` folder and count the types of node and edge.

<div data-lang="python" markdown="1">
{% highlight python %}
#
# Load the nodes from disk and cache. GraphFrames likes nodes/vertices and edges/relatonships to be cached.
#

# We created these in stackexchange.py from Stack Exchange data dump XML files
NODES_PATH: str = f"{BASE_PATH}/Nodes.parquet"
nodes_df: DataFrame = spark.read.parquet(NODES_PATH).cache()

# What kind of nodes we do we have to work with?
node_counts = (
    nodes_df
    .select("id", F.col("Type").alias("Node Type"))
    .groupBy("Node Type")
    .count()
    .orderBy(F.col("count").desc())
)
node_counts.show()

# We created these in stackexchange.py from Stack Exchange data dump XML files
EDGES_PATH: str = f"{BASE_PATH}/Edges.parquet"
edges_df: DataFrame = spark.read.parquet(EDGES_PATH).cache()

# What kind of edges do we have to work with?
edge_counts = (
    edges_df
    .select("src", "dst", F.col("relationship").alias("Edge Type"))
    .groupBy("Edge Type")
    .count()
    .orderBy(F.col("count").desc())
)
edge_counts.show()
{% endhighlight %}
</div>

The counts of the types of nodes are displayed.

<div data-lang="python" markdown="1">
{% highlight python %}
+---------+-----+
|Node Type|count|
+---------+-----+
|    Badge|43029|
|     Vote|42593|
|     User|37709|
|     Post| 5003|
|PostLinks| 1274|
|      Tag|  143|
+---------+-----+

+---------+-----+
|Edge Type|count|
+---------+-----+
|    Earns|43029|
|  CastFor|40701|
|  Answers| 5745|
|     Tags| 4427|
|     Asks| 1934|
|    Links| 1268|
+---------+-----+
{% endhighlight %}
</div>



<div data-lang="python" markdown="1">
{% highlight python %}

{% endhighlight %}
</div>

Now we create a [GraphFrame object](https://graphframes.github.io/graphframes/docs/_site/api/python/graphframes.html#graphframes.GraphFrame) from the `nodes_df` and `edges_df` `DataFrames`. We will use this object to find motifs in the graph. There are many fields in the nodes of our `GraphFrame` because there only one node type is available. This makes it necessary to create a `Type` field for each type of node, and to merge all fields into a single, global `nodes_df` `DataFrame`. The `Type` column can then be used in relational `DataFrame` operations to distinguish between types of nodes. This is an annoyance that should be fixed in the near future, with the ability to have multiple node types in a `GraphFrame`. In practice it isn't a big hit in productivity, it means you have to [DataFrame.select](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.select.html) certain columns for each node `Type` when you do a [DataFrame.show()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.show.html) or the width of the DataFrame will be too wide to easily read.

<div data-lang="python" markdown="1">
{% highlight python %}
g = GraphFrame(nodes_df, edges_df)

# Add the degree to use as a property in the motifs
g = add_degree(g).cache()

g.vertices.show(10)
print(f"Node columns: {g.vertices.columns}")

g.edges.show(10)
{% endhighlight %}

The `GraphFrame` object is created and the node columns and edges are displayed.

<div data-lang="python" markdown="1">
{% highlight python %}
# Node DataFrame is too wide to display here... because it has this many columns.
Node columns: ['id', 'AboutMe', 'AcceptedAnswerId', 'AccountId', 'AnswerCount', 'Body', 'Class', 'ClosedDate', 'CommentCount', 'CommunityOwnedDate', 'ContentLicense', 'Count', 'CreationDate', 'Date', 'DisplayName', 'DownVotes', 'ExcerptPostId', 'FavoriteCount', 'Id', 'IsModeratorOnly', 'IsRequired', 'LastAccessDate', 'LastActivityDate', 'LastEditDate', 'LastEditorDisplayName', 'LastEditorUserId', 'LinkTypeId', 'Location', 'Name', 'OwnerDisplayName', 'OwnerUserId', 'ParentId', 'PostId', 'PostTypeId', 'RelatedPostId', 'Reputation', 'Score', 'TagBased', 'TagName', 'Tags', 'Text', 'Title', 'Type', 'UpVotes', 'UserDisplayName', 'UserId', 'ViewCount', 'Views', 'VoteType', 'VoteTypeId', 'WebsiteUrl', 'WikiPostId', 'degree']

# Edge DataFrame is simpler
+--------------------+--------------------+------------+
|                 src|                 dst|relationship|
+--------------------+--------------------+------------+
|afb0dcb7-1325-441...|7d1fdcf6-52ac-4ee...|     CastFor|
|344c3ebe-6f7b-42a...|3d5aaba5-bf19-49c...|     CastFor|
|b85f6a3e-518e-425...|29e78860-217a-4aa...|     CastFor|
|13def23e-eed5-440...|88ec852f-5887-422...|     CastFor|
|2b4dd817-349c-48c...|266e2b24-78e7-438...|     CastFor|
|486d53ba-7aa7-4ff...|72db9a20-ccc1-431...|     CastFor|
|038afb88-c9f0-496...|e837a1b1-4205-425...|     CastFor|
|5ef6462c-d3f7-4bc...|4dacee4b-8703-4aa...|     CastFor|
|497fcbfb-dd79-421...|6c35e5bf-9a88-455...|     CastFor|
|1c0f584a-2b65-486...|0460e6c8-bb99-4b4...|     CastFor|
+--------------------+--------------------+------------+
only showing top 10 rows
{% endhighlight %}
</div>
