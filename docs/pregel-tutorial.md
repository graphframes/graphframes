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

<h1 id="pagerank">Implementing PageRank with aggregateMesssages</h1>

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
