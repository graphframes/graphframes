#!/bin/bash

set -euxo pipefail

cd ./docs
SKIP_SCALADOC=0 PRODUCTION=1 jekyll build
