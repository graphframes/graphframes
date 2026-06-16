#!/usr/bin/python


import os
import shutil
import subprocess
import sys
from pathlib import Path

import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("spark", type=str, default="4.1.2")
    args = parser.parse_args()
    spark = str(args.spark)

    prj_root = Path(__file__).parent.parent.parent
    scala_root = prj_root.joinpath("connect")
    tmp_dir = prj_root.joinpath("tmp")
    unpacked_spark_binary = f"spark-{spark}-bin-hadoop3-connect"
    spark_home = tmp_dir.joinpath(unpacked_spark_binary)

    os.chdir(spark_home)

    checkpoint_dir = Path("/tmp/GFTestsCheckpointDir")

    stop_connect_cmd = ["./sbin/stop-connect-server.sh"]
    print("Stopping SparkConnect Server...")
    spark_connect_stop = subprocess.run(
        stop_connect_cmd,
        stdout=subprocess.PIPE,
        universal_newlines=True,
    )

    if spark_connect_stop.returncode == 0:
        print("Done.")
    else:
        print("Something goes wrong...")
        sys.exit(1)

    shutil.rmtree(checkpoint_dir.absolute().__str__(), ignore_errors=True)
    sys.exit(0)
