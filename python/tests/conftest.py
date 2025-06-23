import os
import pathlib
import tempfile
from typing import Optional, Tuple
import warnings

import pytest
from pyspark.sql import SparkSession
from pyspark.version import __version__

from graphframes import GraphFrame
from graphframes.classic.graphframe import _java_api

if __version__[:3] >= "3.4":
    from pyspark.sql.utils import is_remote
else:

    def is_remote() -> bool:
        return False
    
spark_major_version = __version__[:1]
scala_version = os.environ.get("SCALA_VERSION", "2.12" if __version__ < "4" else "2.13")


def get_gf_jar_locations() -> Tuple[str, str]:
    """
    Returns a location of the GraphFrames JAR and GraphFrames Connect JAR.

    In the case your version of PySpark is not compatible with the version of GraphFrames,
    this function will raise an exception!
    """
    project_root = pathlib.Path(__file__).parent.parent.parent
    core_dir = project_root / "target" / f"scala-{scala_version}"
    connect_dir = project_root / "graphframes-connect" / "target" / f"scala-{scala_version}"

    core_jar: Optional[str] = None
    connect_jar: Optional[str] = None

    for pp in core_dir.glob(f"graphframes-spark-{spark_major_version}-assembly-*.jar"):
        assert isinstance(pp, pathlib.PosixPath)  # type checking
        core_jar = str(pp.absolute())

    for pp in connect_dir.glob(f"graphframes-connect-spark-{spark_major_version}-assembly-*.jar"):
        assert isinstance(pp, pathlib.PosixPath)  # type checking
        connect_jar = str(pp.absolute())

    print(core_jar, connect_jar)

    if core_jar is None or connect_jar is None:
        raise ValueError(
            f"Failed to find graphframes jars for Spark {spark_major_version}"
        )
    
    return (core_jar, connect_jar)


@pytest.fixture(scope="module")
def spark():
    checkpointDir = "/tmp/GFTestsCheckpointDir"
    warnings.filterwarnings("ignore", category=ResourceWarning)
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    pathlib.Path(checkpointDir).mkdir(parents=True, exist_ok=True)

    (core_jar, connect_jar) = get_gf_jar_locations()

    with tempfile.TemporaryDirectory() as tmp_dir:
        builder = (SparkSession.Builder()
            .appName("GraphFramesTest")
            .config("spark.sql.shuffle.partitions", 4)
            .config("spark.checkpoint.dir", tmp_dir)
            .config("spark.jars", f"{core_jar},{connect_jar}")
        )

        if spark_major_version == "3":
            # Spark 3 does not include connect by default
            builder = builder.config("spark.jars.packages", f"org.apache.spark:spark-connect_{scala_version}:{__version__}")

        if is_remote():
            builder = (builder
                .remote("local")
                .config("spark.connect.extensions.relation.classes", "org.apache.spark.sql.graphframes.GraphFramesConnect")
            )
        else:
            builder = builder.master("local[4]")

        spark = builder.getOrCreate()
        yield spark
        spark.stop()


@pytest.fixture(scope="module")
def local_g(spark):
    localVertices = [(1, "A"), (2, "B"), (3, "C")]
    localEdges = [(1, 2, "love"), (2, 1, "hate"), (2, 3, "follow")]
    v = spark.createDataFrame(localVertices, ["id", "name"])
    e = spark.createDataFrame(localEdges, ["src", "dst", "action"])
    yield GraphFrame(v, e)


@pytest.fixture(scope="module")
def examples(spark):
    if is_remote():
        # TODO: We should update tests to be able to run all of them on Spark Connect
        # At the moment the problem is that examples API is py4j based.
        yield None
    else:
        japi = _java_api(spark._sc)
        yield japi.examples()
