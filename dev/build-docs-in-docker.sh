#!/bin/bash

set -euxo pipefail

cd ./docs
SKIP_SCALADOC=1 PRODUCTION=1 jekyll build
