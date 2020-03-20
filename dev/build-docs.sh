#!/bin/bash

set -euxo pipefail

docker build -t databricks/graphframes .

# build the docs image
docker build -t databricks/graphframes-docs docs/

# build the API docs
# TODO fix docker on linux to only create files as current user.
docker run --rm \
    -v "$(pwd):/mnt/graphframes" \
    -v "$HOME/.sbt:/root/.sbt" \
    -v "$HOME/.ivy2:/root/.ivy2" \
    databricks/graphframes-docs bash -i -c dev/build-docs-in-docker.sh
