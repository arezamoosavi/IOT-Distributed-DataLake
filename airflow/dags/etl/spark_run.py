import os
import sys
import logging

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from delta.tables import DeltaTable


def get_spark_session(appname, hive_metastore, minio_url,
                      minio_access_key, minio_secret_key):

    spark = (SparkSession.builder
             .appName(appname)
             .config("spark.network.timeout", "10000s")
             .config("hive.metastore.uris", hive_metastore)
             .config("hive.exec.dynamic.partition", "true")
             .config("hive.exec.dynamic.partition.mode", "nonstrict")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "true")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.fast.upload", "true")
             .config("spark.hadoop.fs.s3a.endpoint", minio_url)
             .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
             .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.history.fs.logDirectory", "s3a://spark/")
             .config("spark.sql.files.ignoreMissingFiles", "true")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
             .enableHiveSupport()
             .getOrCreate())
    return spark


# spark session
spark = get_spark_session("ETL", "thrift://hive:9083", "http://minio:9000",
                          "spark", "spark12345")
# Set log4j
spark.sparkContext.setLogLevel("ERROR")
log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger("LOGGER")
logger.setLevel(log4jLogger.Level.INFO)


spark.read.format("delta").load("s3a://datalake/deltatables/events/").show(10)

spark.sql(
    "CREATE TABLE IF NOT EXISTS temp_events USING DELTA LOCATION 's3a://datalake/deltatables/events/'")
spark.sql("""SELECT COUNT(*) AS count FROM temp_events WHERE type = 'T2' """).show()

spark.stop()
