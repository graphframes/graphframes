#!/usr/bin/python

import os
import shutil
import subprocess
import sys
from pathlib import Path

import argparse

SPARK_ARCHIVE_LINK = "https://dlcdn.apache.org/spark/spark-{}/spark-{}-bin-hadoop3-connect.tgz"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("spark", type=str, default="4.1.2")
    args = parser.parse_args()
    spark = str(args.spark)

    spark_full_link = SPARK_ARCHIVE_LINK.format(spark, spark)
    
    prj_root = Path(__file__).parent.parent.parent
    scala_root = prj_root.joinpath("connect")

    print("Build Graphframes...")
    os.chdir(prj_root)

    build_command = ["./build/sbt", f"-Dspark.version={spark}", "connect/clean", "+", "connect/assembly"]
    build_sbt = subprocess.run(
        build_command,
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

    tmp_dir = prj_root.joinpath("tmp")
    tmp_dir.mkdir(exist_ok=True)
    os.chdir(tmp_dir)

    spark_archive = spark_full_link.split("/")[-1]
    unpackaed_spark_binary = spark_archive[:-4]
    
    if not tmp_dir.joinpath(unpackaed_spark_binary).exists():
        print(f"Downloading spark {spark}...")
        if tmp_dir.joinpath(spark_archive).exists():
            shutil.rmtree(
                tmp_dir.joinpath(spark_archive),
                ignore_errors=True,
            )
        print(f"Link is resolved to {spark_full_link}")

        get_spark = subprocess.run(
            [
                "wget",
                "--no-verbose",
                spark_full_link,
            ],
            stdout=subprocess.PIPE,
            universal_newlines=True,
        )
        if get_spark.returncode == 0:
            print("Done.")
        else:
            print("Downlading failed.")
            print("stdout: ", get_spark.stdout)
            print("stdeerr: ", get_spark.stderr)
            sys.exit(1)

        print("Unpacking Spark...")
        unpack_spark = subprocess.run(
            [
                "tar",
                "-xzf",
                spark_archive,
            ],
            stdout=subprocess.PIPE,
            universal_newlines=True,
        )
        if unpack_spark.returncode == 0:
            print("Done.")
        else:
            print("Unpacking failed.")
            print("stdout: ", unpack_spark.stdout)
            print("stdeerr: ", unpack_spark.stderr)
            sys.exit(1)

    spark_home = tmp_dir.joinpath(unpackaed_spark_binary)
    os.chdir(spark_home)

    gf_jar = None
    scala_target_dir = scala_root.joinpath("target").joinpath("scala-2.13")
    print(f"looking for the connect asembly in {scala_target_dir.absolute()}")
    for ff in scala_target_dir.glob("graphframes-connect-spark4*"):
        gf_jar = ff
        break

    if gf_jar is None:
        raise ValueError("faile to locate connect assembly JAR")
    
    _ = shutil.copyfile(gf_jar, spark_home.joinpath(gf_jar.name))
    checkpoint_dir = Path("/tmp/GFTestsCheckpointDir")
    if checkpoint_dir.exists():
        shutil.rmtree(checkpoint_dir.absolute().__str__(), ignore_errors=True)

    checkpoint_dir.mkdir(exist_ok=True, parents=True)

    run_connect_command = [
        "./sbin/start-connect-server.sh",
        "--jars",
        f"{gf_jar.name}",
        "--conf",
        "spark.connect.extensions.relation.classes=org.apache.spark.sql.graphframes.GraphFramesConnect",
        "--conf",
        "spark.checkpoint.dir=/tmp/GFTestsCheckpointDir",
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
    else:
        print("Failed to start SparkConnect server.")
        sys.exit(1)
