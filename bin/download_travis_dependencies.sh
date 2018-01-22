#!/usr/bin/env bash

set -eux -o pipefail

SPARK_BUILD="spark-${SPARK_VERSION}-bin-hadoop2.7"

function try_download_from_apache {
    local spark_tarball="${SPARK_BUILD}.tgz"
    local apache_archive_prefix="https://archive.apache.org/dist"
    local spark_rel_path="spark/spark-${SPARK_VERSION}/${spark_tarball}"
    local spark_bin_url="${apache_archive_prefix}/${spark_rel_path}"
    local spark_md5_url="${apache_archive_prefix}/${spark_rel_path}.md5"

    echo "Downloading Spark if necessary"
    echo "Spark version = $SPARK_VERSION"
    echo "Spark build = $SPARK_BUILD"
    echo "Spark build URL = ${spark_bin_url}"

    # Existing files might be corrupt, clean them up.
    rm -f "${spark_tarball}" "${spark_tarball}.md5"

    curl --retry 3 --retry-delay 7 -O "${spark_bin_url}"
    curl --retry 3 --retry-delay 7 -O "${spark_md5_url}"

    echo "Content of directory:"
    ls -la
    gpg --print-md MD5 "${spark_tarball}" | tee "${spark_tarball}.gen.md5"
    diff "${spark_tarball}.gen.md5" "${spark_tarball}.md5"
    tar -zxf "${spark_tarball}"
}

mkdir -p "${HOME}/.cache/spark-versions" && pushd $_
try_download_from_apache || try_download_from_apache || try_download_from_apache
popd
