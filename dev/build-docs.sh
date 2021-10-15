#!/bin/bash

set -euxo pipefail

if [[ $(uname -sm) == "Darwin arm64" ]]; then
  # https://github.com/docker/for-mac/issues/5419#issuecomment-834624859
  PLATFORM_OPT="--platform=linux/arm64"
else
  PLATFORM_OPT=
fi

docker build $PLATFORM_OPT -t graphframes/dev .

# build the docs image
docker build $PLATFORM_OPT -t graphframes/docs docs/

# build the API docs
# TODO fix docker on linux to only create files as current user.
docker run $PLATFORM_OPT --rm \
    -v "$(pwd):/mnt/graphframes" \
    -v "$HOME/.sbt:/root/.sbt" \
    -v "$HOME/.ivy2:/root/.ivy2" \
    graphframes/docs bash -i -c dev/build-docs-in-docker.sh
