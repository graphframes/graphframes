#!/bin/bash

set -euxo pipefail

# We use python2.7 for building the docs because Pygments.rb/Pygments is incompatible with
# python 3, more info: https://github.com/jekyll/jekyll/issues/2604
docker build -t databricks/graphframes --build-arg PYTHON_VERSION=2.7 .

# build the docs image
docker build -t databricks/graphframes-docs docs/

# build the API docs
# TODO fix docker on linux to only create files as current user.
docker run --rm \
    -v "$(pwd):/mnt/graphframes" \
    -v "$HOME/.sbt:/root/.sbt" \
    -v "$HOME/.ivy2:/root/.ivy2" \
    databricks/graphframes-docs bash -i -c dev/build-docs-in-docker.sh
