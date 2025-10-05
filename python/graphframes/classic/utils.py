from py4j.java_gateway import JavaObject
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel


def storage_level_to_jvm(storage_level: StorageLevel, spark: SparkSession) -> JavaObject:
    return spark._jvm.org.apache.spark.storage.StorageLevel.apply(
        storage_level.useDisk,
        storage_level.useMemory,
        storage_level.useOffHeap,
        storage_level.deserialized,
        storage_level.replication,
    )
