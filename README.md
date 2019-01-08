# graphframes
[![Build Status](https://travis-ci.org/graphframes/graphframes.svg?branch=master)](https://travis-ci.org/graphframes/graphframes)
[![codecov.io](http://codecov.io/github/graphframes/graphframes/coverage.svg?branch=master)](http://codecov.io/github/graphframes/graphframes?branch=master)


# GraphFrames: DataFrame-based Graphs

This is a package for DataFrame-based graphs on top of Apache Spark.
Users can write highly expressive queries by leveraging the DataFrame API, combined with a new
API for motif finding.  The user also benefits from DataFrame performance optimizations
within the Spark SQL engine.

You can find user guide and API docs at https://graphframes.github.io/graphframes.

## Building and running unit tests

To compile this project, run `build/sbt assembly` from the project home directory.
This will also run the Scala unit tests.

To run the Python unit tests, run the `run-tests.sh` script from the `python/` directory.
You will need to set `SPARK_HOME` to your local Spark installation directory.

## Spark version compatibility

This project is compatible with Spark 2.3+.  However, significant speed improvements have been
made to DataFrames in more recent versions of Spark, so you may see speedups from using the latest
Spark version.

## Contributing

GraphFrames is collaborative effort among UC Berkeley, MIT, and Databricks.
We welcome open source contributions as well!

## Releases:

See [release notes](https://github.com/graphframes/graphframes/releases).
