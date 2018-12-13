#!/bin/bash

set -euxo pipefail

# We use python2.7 for building the docs because Pygments.rb/Pygments is incompatible with Python
# 3, more info: https://github.com/jekyll/jekyll/issues/2604
docker build -t databricks/graphframes --build-arg PYTHON_VERSION=2.7 .

# build the docs image
docker build -t databricks/graphframes-docs docs/

# create the assembly jar on host because we have ivy/maven cache
build/sbt 'set test in assembly := {}' assembly

# build the API docs
# TODO fix docker on linux to only create files as current user.
docker run --rm -v "$(pwd):/mnt/graphframes" databricks/graphframes-docs \
    bash -i -c dev/build-docs-in-docker.sh
