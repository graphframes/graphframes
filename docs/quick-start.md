---
layout: global
displayTitle: GraphFrames Quick-Start Guide
title: Quick-Start Guide
description: GraphFrames GRAPHFRAMES_VERSION guide for getting started quickly
---

This quick-start guide shows how to get started using GraphFrames.
After you work through this guide, move on to the [User Guide](user-guide.html)
to learn more about the many queries and algorithms supported by GraphFrames.

* Table of contents
{:toc}

# Getting started with Spark and Spark packages

If you are new to using Spark, refer to the
[Spark Documentation](http://spark.apache.org/docs/latest/index.html) and its
[Quick-Start Guide](http://spark.apache.org/docs/latest/quick-start.html) for more information.

If you are new to using [Spark packages](http://spark-packages.org/), you can find more information
in the [Spark User Guide on using the interactive shell](http://spark.apache.org/docs/latest/programming-guide.html#using-the-shell).
You just need to make sure your Spark shell session has the package as a dependency.

The following example shows how to run the Spark shell with the GraphFrames package.

<div class="codetabs">

<div data-lang="scala"  markdown="1">

If you have GraphFrames available as a JAR `graphframes.jar`, you can add the JAR to the shell
classpath:

{% highlight bash %}
$ ./bin/spark-shell --master local[4] --jars graphframes.jar
{% endhighlight %}

{:/TODO: maven coordinates when published}
</div>

<div data-lang="python"  markdown="1">

If you have GraphFrames available as a JAR `graphframes.jar`, you can make GraphFrames available
by passing the JAR to the pyspark shell script as follows:

{% highlight bash %}
$ ./bin/pyspark --master local[4] --py-files graphframes.jar --jars graphframes.jar
{% endhighlight %}

{:/TODO: maven coordinates when published}
</div>

</div>

# Start using GraphFrames

The following example shows how to create a GraphFrame, query it, and run the PageRank algorithm.

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
TODO
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
TODO
{% endhighlight %}
</div>

</div>
