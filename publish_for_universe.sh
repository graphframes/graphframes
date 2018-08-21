#!/usr/bin/env bash

set -euo pipefail

# Usage: ./publish_for_universe.sh [local/remote]
# Run this script from the repository's base directory.
# This script is for publishing this library for use in universe.
# See TODO:LINK for more info.

# Verify number of command-line arguments
if [[ $# -eq 0 ]] || [[ $# -gt 1 ]]; then
    echo "Usage:  publish_for_universe.sh [mode]
        mode: 'local': Publish JAR to the local maven repository
              'remote': Publish JAR to the Databricks Maven repository
    Run this script from this repository's base directory."
    exit 1
fi

packageName=graphframes

# Parse command line arguments (code from https://stackoverflow.com/a/14203146)
while [[ $# -gt 0 ]]
do
key="$1"
case "${key}" in
    local)
    LOCAL=true
    ;;
    remote)
    LOCAL=false
    ;;
esac
shift # past argument or value
done

# Check that LOCAL was specified
if [[ -z "${LOCAL:-}" ]]; then
    echo "MODE is unset.  Specify it as an argument 'local' or 'remote'"
    exit 1
fi

# Log whether we're publishing JAR locally or to maven
if [[ "${LOCAL}" == "true" ]]; then
    echo "Publishing $packageName JAR (for Universe) locally"
else
    echo "Publishing $packageName JAR (for Universe) to Databricks maven repository."
fi

function show_publish_instructions {
    echo "======================================================================="
    echo "If this fails, you may need to set up your AWS credentials."
    echo "======================================================================="
    exit 1
}

if [[ "${LOCAL}" == "true" ]]; then
    publish_cmd="publishM2"
else
    publish_cmd="publish"
fi
./build/sbt \
    assembly "${publish_cmd}" \
    || show_publish_instructions
