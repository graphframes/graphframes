import pathlib
import shutil
import warnings
from importlib import resources

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


@pytest.fixture(scope="module")
def spark():
    checkpointDir = "/tmp/GFTestsCheckpointDir"
    spark = SparkSession.getActiveSession()
    if spark is not None:
        yield spark
        spark.stop()
        shutil.rmtree(checkpointDir, ignore_errors=True)
    warnings.filterwarnings("ignore", category=ResourceWarning)
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    pathlib.Path(checkpointDir).mkdir(parents=True, exist_ok=True)
    if is_remote():
        spark = (
            SparkSession.builder.remote("sc://localhost:15002")
            .appName("GraphFramesTest")
            .config("spark.sql.shuffle.partitions", 4)
            .config("spark.checkpoint.dir", checkpointDir)
            .getOrCreate()
        )
        yield spark
        spark.stop()
    else:
        spark_builder = SparkSession.builder.master("local[4]").config(
            "spark.sql.shuffle.partitions", 4
        )
        resources_root = resources.files("graphframes").joinpath("resources")
        spark_jars = []
        for pp in resources_root.iterdir():
            assert isinstance(pp, pathlib.PosixPath)  # type checking
            if pp.is_file() and pp.name.endswith(".jar"):
                spark_jars.append(pp.absolute().__str__())
        if spark_jars:
            jars_str = ",".join(spark_jars)
            spark = spark_builder.config("spark.jars", jars_str)
        spark = spark_builder.getOrCreate()
        spark.sparkContext.setCheckpointDir(checkpointDir)
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
