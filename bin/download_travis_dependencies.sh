#!/usr/bin/env bash

set -euo pipefail

SPARK_BUILD="spark-${SPARK_VERSION}-bin-hadoop2.7"

function maybe_download_from_apache_mirror {
    local spark_tarball="${SPARK_BUILD}.tgz"
    local apache_mirror_cgi="https://www.apache.org/dyn/closer.lua"
    local spark_rel_path="spark/spark-${SPARK_VERSION}/${spark_tarball}"
    local spark_url="${apache_mirror_cgi}?path=${spark_rel_path}"

    echo "Downloading Spark if necessary"
    echo "Spark version = $SPARK_VERSION"
    echo "Spark build = $SPARK_BUILD"
    echo "Spark build URL = $spark_url"

    if [[ ! -f "${spark_tarball}" ]]; then
        python -- << __PY_SCRIPT_EOF__ | xargs curl -O
import sys, json
json_str = """$(curl --silent --location "${spark_url}&asjson=1")"""
pkg_info = json.loads(json_str)
print("{}/${spark_rel_path}".format(pkg_info["preferred"]))
__PY_SCRIPT_EOF__

        echo "Content of directory:"
        ls -la
        tar xvf "${spark_tarball}" > /dev/null
    fi
}

mkdir -p "${HOME}/.cache/spark-versions" && pushd $_
maybe_download_from_apache_mirror
popd
