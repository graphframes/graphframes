# GraphFrames: DataFrame-based Graphs
[![Build Status](https://travis-ci.org/graphframes/graphframes.svg?branch=master)](https://travis-ci.org/graphframes/graphframes)
[![codecov.io](http://codecov.io/github/graphframes/graphframes/coverage.svg?branch=master)](http://codecov.io/github/graphframes/graphframes?branch=master)

This is a package for DataFrame-based graphs on top of Apache Spark. Users can
write highly expressive queries by leveraging the DataFrame API, combined with
a new API for motif finding. The user also benefits from DataFrame performance
optimizations within the Spark SQL engine.

You can find user guide and API docs at the [GraphFrames home
page](https://graphframes.github.io/graphframes).

## Building and running unit tests

To compile this project, run `build/sbt assembly` from the project home
directory. This will also run the Scala unit tests.

o run the Python unit tests, run the `run-tests.sh` script from the `python/`
directory. You will need to set `SPARK_HOME` to your local Spark installation
directory.

## Spark version compatibility

This project is compatible with Spark 2.4+. However, significant speed
improvements have been made to DataFrames in more recent versions of Spark, so
you may see speedups from using the latest Spark version.

## Contributing

GraphFrames is collaborative effort among UC Berkeley, MIT, and Databricks.
We welcome open source contributions as well!

## Releases

See [release notes](https://github.com/graphframes/graphframes/releases).

## Setting up pre-commit hooks

This repository contains [pre-commit][pre-commit] configuration. The hooks
executed by `pre-commit` will validate your CircleCI configuration files
before you push them to GitHub.

To activate the hooks on your workstation, please install the
[asdf][guidebook-asdf] tool first. Once you have `asdf` on your system, use it
to install the [pre-commit][guidebook-pre-commit] plugin.

Then activate the pre-commit hooks in the current git checkout:

```shell
pre-commit install
```

You can disable the pre-commit hooks by running

```shell
pre-commit uninstall
```

[guidebook-asdf]: https://backstage.lobby.liveintenteng.com/docs/default/component/engineering-guidebook/workstation-setup/utility-packages/#package-asdf-vm
[guidebook-pre-commit]: https://backstage.lobby.liveintenteng.com/docs/default/component/engineering-guidebook/workstation-setup/utility-packages/#pre-commit-application-and-its-setup
[pre-commit]: https://pre-commit.com/