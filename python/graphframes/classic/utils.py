from py4j.java_gateway import JavaObject
from pyspark.storagelevel import StorageLevel
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.classic.dataframe import SparkSession


def storage_level_to_jvm(storage_level: StorageLevel, spark: SparkSession) -> JavaObject:
    return spark._jvm.org.apache.spark.storage.StorageLevel.apply(
        storage_level.useDisk,
        storage_level.useMemory,
        storage_level.useOffHeap,
        storage_level.deserialized,
        storage_level.replication,
    )
