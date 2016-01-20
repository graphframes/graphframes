#spark-df-graph

# DFGraph: DataFrame-based Graphs

This is a prototype package for DataFrame-based graphs in Spark.
Users can write highly expressive queries by leveraging the DataFrame API, combined with a new
API for motif finding.  The user also benefits from DataFrame performance optimizations
within the Spark SQL engine.

**Collaboration and prototyping**: This package is based on the GraphFrames project led by
[Ankur Dave](http://ankurdave.com/).  The goals of this package are (a) to provide this
powerful API to users earlier and (b) to gather feedback about the API and use cases
to help the GraphFrames project.

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
