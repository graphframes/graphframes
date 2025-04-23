import shutil
import subprocess
import sys
from pathlib import Path


def build(spark_version: str = "3.5.4"):
    print("Building GraphFrames JAR...")
    print(f"SPARK_VERSION: {spark_version[:3]}")
    assert spark_version[:3] in {"3.3", "3.4", "3.5"}, "Unsopported spark version!"
    spark_major_version = spark_version[0]
    project_root = Path(__file__).parent.parent.parent
    sbt_executable = project_root.joinpath("build").joinpath("sbt").absolute().__str__()
    sbt_build_command = [sbt_executable, f"-Dspark.version={spark_version}", "assembly"]
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
        if f"graphframes-spark-{spark_major_version}-assembly" in pp.name:
            gf_jar = pp
            break

    assert gf_jar is not None, "Missing JAR!"
    python_resources.mkdir(parents=True, exist_ok=True)
    shutil.copy(gf_jar, python_resources.joinpath(gf_jar.name))


if __name__ == "__main__":
    if len(sys.argv) > 1:
        spark_version = sys.argv[1]
        build(spark_version)
    else:
        build()
