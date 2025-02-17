# graphframes
[![Scala CI](https://github.com/graphframes/graphframes/actions/workflows/scala-ci.yml/badge.svg)](https://github.com/graphframes/graphframes/actions/workflows/scala-ci.yml)
[![Python CI](https://github.com/graphframes/graphframes/actions/workflows/python-ci.yml/badge.svg)](https://github.com/graphframes/graphframes/actions/workflows/python-ci.yml)
[![pages-build-deployment](https://github.com/graphframes/graphframes/actions/workflows/pages/pages-build-deployment/badge.svg)](https://github.com/graphframes/graphframes/actions/workflows/pages/pages-build-deployment)


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

## Release new version
Please see guide `dev/release_guide.md`.

## Spark version compatibility

This project is compatible with Spark 2.4+.  However, significant speed improvements have been
made to DataFrames in more recent versions of Spark, so you may see speedups from using the latest
Spark version.

## Contributing

GraphFrames is collaborative effort among UC Berkeley, MIT, and Databricks.
We welcome open source contributions as well!

## Releases:

See [release notes](https://github.com/graphframes/graphframes/releases).
