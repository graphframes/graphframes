#!/usr/bin/python

# Inspired by https://github.com/mrpowers-io/tsumugi-spark/blob/main/dev/run-connect.py

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pyspark

SBT_BUILD_COMMAND = ["./build/sbt", "connect/assembly"]
SPARK_VERSION = "3.5.5"
SCALA_VERSION = "2.12"


if __name__ == "__main__":
    prj_root = Path(__file__).parent.parent.parent
    scala_root = prj_root.joinpath("graphframes-connect")

    print("Build Graphframes...")
    os.chdir(prj_root)
    build_sbt = subprocess.run(
        SBT_BUILD_COMMAND,
        stdout=subprocess.PIPE,
        universal_newlines=True,
    )

    if build_sbt.returncode == 0:
        print("Done.")
    else:
        print(f"SBT build return an error: {build_sbt.returncode}")
        print("stdout: ", build_sbt.stdout)
        print("stderr: ", build_sbt.stderr)
        sys.exit(1)

    spark_home = Path(pyspark.__path__[0])
    os.chdir(spark_home)

    gf_jars = (
        scala_root.joinpath("target")
        .joinpath(f"scala-{SCALA_VERSION}")
    )
    gf_jar = [pp for pp in gf_jars.glob("*.jar") if "graphframes-connect-assembly" in pp.name][0]

    checkpoint_dir = Path("/tmp/GFTestsCheckpointDir")
    if checkpoint_dir.exists():
        shutil.rmtree(checkpoint_dir.absolute().__str__(), ignore_errors=True)

    checkpoint_dir.mkdir(exist_ok=True, parents=True)

    run_connect_command = [
        "./sbin/spark-daemon.sh",
        "submit",
        "org.apache.spark.sql.connect.service.SparkConnectServer",
        "1",
        "--name",
        "Spark Connect server",
        "--jars",
        str(gf_jar.absolute()),
        "--conf",
        "spark.connect.extensions.relation.classes=org.apache.spark.sql.graphframes.GraphFramesConnect",
        "--conf",
        "spark.checkpoint.dir=/tmp/GFTestsCheckpointDir",
        "--packages",
        f"org.apache.spark:spark-connect_{SCALA_VERSION}:{SPARK_VERSION}",
    ]
    print("Starting SparkConnect Server...")
    spark_connect = subprocess.run(
        run_connect_command,
        stdout=subprocess.PIPE,
        universal_newlines=True,
    )

    if spark_connect.returncode == 0:
        print("Done.")
        sys.exit(0)
