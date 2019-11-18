#!/usr/bin/env bash

set -eux -o pipefail

SPARK_BUILD="spark-${SPARK_VERSION}-bin-hadoop2.7"

_script_dir_="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

spark_tarball="${SPARK_BUILD}.tgz"

function try_download_from_apache {
    local apache_mirror_cgi="https://www.apache.org/dyn/closer.lua"
    local spark_rel_path="spark/spark-${SPARK_VERSION}/${spark_tarball}"
    local spark_url="${apache_mirror_cgi}?path=${spark_rel_path}"

    echo "Spark build URL = $spark_url"

    # Grab the actual download location from the Apache mirror's gateway.
    # The JSON field "preferred" stores this address.
    curl --silent --location "${spark_url}&asjson=1" | \
        python <(cat << __PY_SCRIPT_EOF__
import sys, json
pkg_info = json.load(sys.stdin)
print("{}/${spark_rel_path}".format(pkg_info["preferred"]))
__PY_SCRIPT_EOF__
) | xargs curl --retry 3 --retry-delay 7 -O
}

function try_download_latest_snapshot {
    local spark_url="https://ml-team-public-read.s3-us-west-2.amazonaws.com/spark-3.0.0-SNAPSHOT-bin-hadoop2.7.tgz"
    echo "Spark build URL = $spark_url"
    wget --tries=3 ${spark_url}
}

echo "Downloading Spark if necessary"
echo "Spark version = $SPARK_VERSION"
echo "Spark build = $SPARK_BUILD"

mkdir -p "${HOME}/.cache/spark-versions" && pushd $_

# Remove existing Spark tarball in case it is corrupted.
rm -f "${spark_tarball}"
# Remove existing Spark extracted directory
rm -rf "${SPARK_BUILD}"

if [ ${SPARK_VERSION} = '3.0.0-SNAPSHOT' ]; then
    try_download_latest_snapshot
else
    try_download_from_apache || try_download_from_apache || try_download_from_apache
fi;

echo "Content of directory:"
ls -la
tar -zxf "${spark_tarball}"

popd
