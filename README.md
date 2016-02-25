#graphframes
[![Build Status](https://travis-ci.org/graphframes/graphframes.svg?branch=master)](https://travis-ci.org/graphframes/graphframes)


# GraphFrames: DataFrame-based Graphs

This is a prototype package for DataFrame-based graphs on top of Apache Spark.
Users can write highly expressive queries by leveraging the DataFrame API, combined with a new
API for motif finding.  The user also benefits from DataFrame performance optimizations
within the Spark SQL engine.

**Merging with Spark**: The ultimate goal is to provide DataFrame-based graphs in Spark itself.
This package will provide a public API for this work, until the time when this work can be
merged into Spark itself.

## Building and running unit tests

To compile this project, run `build/sbt assembly` from the project home directory.
This will also run the Scala unit tests.

To run the Python unit tests, run the `run-tests.sh` script from the `python/` directory.
You will need to set `SPARK_HOME` to your local Spark installation directory.

## Spark version compatibility

This project is compatible with Spark 1.4+.  However, significant speed improvements have been
made to DataFrames in more recent versions of Spark, so you may see speedups from using the latest
Spark version.

## Contributing

GraphFrames is collaborative effort among UC Berkeley, MIT, and Databricks.
We welcome open source contributions as well!
