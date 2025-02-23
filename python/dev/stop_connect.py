#!/usr/bin/python


import shutil
import subprocess
from pathlib import Path

if __name__ == "__main__":
    prj_root = Path(__file__).parent.parent.parent
    scala_root = prj_root.joinpath("graphframes-connect")
    tmp_dir = prj_root.joinpath("tmp")
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
