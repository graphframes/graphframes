#!/usr/bin/python


import os
import shutil
import subprocess
import sys
from pathlib import Path

import pyspark

SPARK_VERSION = "3.5.5"

if __name__ == "__main__":
    prj_root = Path(__file__).parent.parent.parent
    spark_home = Path(pyspark.__path__[0])

    os.chdir(spark_home)

    checkpoint_dir = Path("/tmp/GFTestsCheckpointDir")

    stop_connect_cmd = [
        "./sbin/spark-daemon.sh",
        "stop",
        "org.apache.spark.sql.connect.service.SparkConnectServer",
        "1",
    ]
    print("Stopping SparkConnect Server...")
    spark_connect_stop = subprocess.run(
        stop_connect_cmd,
        stdout=subprocess.PIPE,
        universal_newlines=True,
    )

    if spark_connect_stop.returncode == 0:
        print("Done.")

    shutil.rmtree(checkpoint_dir.absolute().__str__(), ignore_errors=True)
    sys.exit(0)
