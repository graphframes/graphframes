import pathlib
from importlib import resources

from pyspark.version import __version__

from .graphframe import GraphFrame


def get_gf_jar_location() -> str:
    """
    Returns a location of the GraphFrames JAR,
    included to the distribution of the graphframes-py.

    Usage: just add the returned value of the function to `spark.jars`:
    `SparkSession.builder.master(...).config("spark.jars", get_gf_jar_location()).getOrCreate()`.

    In the case your version of PySpark is not compatible with the version of GraphFrames,
    this function will raise an exception!
    """
    resources_root = resources.files("graphframes").joinpath("resources")

    for pp in resources_root.iterdir():
        assert isinstance(pp, pathlib.PosixPath)  # type checking
        if pp.is_file() and pp.name.endswith(".jar") and __version__[:5] in pp.name:
            return str(pp.absolute())

    raise ValueError(
        f"You version of spark {__version__} is not supported by this version of grpahframes!"
    )


__all__ = ["GraphFrame", "get_gf_jar_location"]
