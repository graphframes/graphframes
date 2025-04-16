import shutil
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path


def build(spark_versions: Sequence[str] = ["3.5.5"]):
    for spark_version in spark_versions:
        print("Building GraphFrames JAR...")
        print(f"SPARK_VERSION: {spark_version[:3]}")
        assert spark_version[:3] in {"3.5",}, "Unsopported spark version!"
        project_root = Path(__file__).parent.parent.parent
        sbt_executable = project_root.joinpath("build").joinpath("sbt").absolute().__str__()
        sbt_build_command = [
            sbt_executable,
            f"-Dspark.version={spark_version}",
            "clean",
            "assembly",
        ]
        sbt_build = subprocess.Popen(
            sbt_build_command,
            stdout=subprocess.PIPE,
            universal_newlines=True,
            cwd=project_root,
        )
        while sbt_build.poll() is None:
            assert sbt_build.stdout is not None  # typing stuff
            line = sbt_build.stdout.readline()
            print(line.rstrip(), flush=True)

        if sbt_build.returncode != 0:
            print("Error during the build of GraphFrames JAR!")
            print("stdout: ", sbt_build.stdout)
            print("stderr: ", sbt_build.stderr)
            sys.exit(1)
        else:
            print("Building DONE successfully!")

        python_resources = (
            project_root.joinpath("python").joinpath("graphframes").joinpath("resources")
        )
        target_dir = project_root.joinpath("target").joinpath("scala-2.12")
        gf_jar = None

        for pp in target_dir.glob("*.jar"):
            if "graphframes-assembly" in pp.name:
                gf_jar = pp
                break

        assert gf_jar is not None, "Missing JAR!"
        shutil.rmtree(python_resources, ignore_errors=True)
        python_resources.mkdir(parents=True, exist_ok=True)
        shutil.copy(gf_jar, python_resources.joinpath(f"spark-{spark_version}-{gf_jar.name}"))


if __name__ == "__main__":
    if len(sys.argv) > 1:
        spark_versions = []
        for arg in sys.argv[1:]:
            spark_versions.append(arg)
        build(spark_versions)
    else:
        build()
