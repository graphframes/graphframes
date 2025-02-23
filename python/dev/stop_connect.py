#!/usr/bin/python


import os
import shutil
import subprocess
from pathlib import Path

SPARK_VERSION = "3.5.4"

if __name__ == "__main__":
    prj_root = Path(__file__).parent.parent.parent
    scala_root = prj_root.joinpath("graphframes-connect")
    tmp_dir = prj_root.joinpath("tmp")
    unpackaed_spark_binary = f"spark-{SPARK_VERSION}-bin-hadoop3"
    spark_home = tmp_dir.joinpath(unpackaed_spark_binary)

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

    shutil.rmtree(checkpoint_dir.absolute().__str__())
