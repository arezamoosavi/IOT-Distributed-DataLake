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

sdf = spark.read.json("s3a://datalake/batch/1/*.json")


def transform_timestamp(ts):
    if "T" in ts:
        date_time = ts.split("T")
        date_str = "-".join([date_time[0][:4], date_time[0]
                             [6:8], date_time[0][4:6]])
        return " ".join([date_str, date_time[1]])
    return ts


udf_transform_timestamp = F.udf(transform_timestamp, "string")

sdf = sdf.withColumn("time", udf_transform_timestamp(
    F.col("utc_timestamp")).cast("string"))

sdf = sdf.withColumn("time", sdf["time"].cast("timestamp"))

sdf = sdf.withColumn(
    "ds",
    F.concat_ws(
        "-",
        F.year(F.col("time")),
        F.month(F.col("time")),
        F.dayofmonth(F.col("time")),
    ),
)

sdf = sdf.select(["id", "device_id", "event_code", "event_detail",
                  "time", "group_id", "desc", "type", "ds"])
sdf.printSchema()

# sdf.filter(sdf.time.isNull()).show()
# sdf.where(sdf.utc_timestamp.contains("T")).show()

sdf.write.format("delta").partitionBy(
    "ds").save('s3a://datalake/deltatables/events/')

spark.stop()
