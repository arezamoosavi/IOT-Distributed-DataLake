import os
import sys
import logging

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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


def transform_timestamp(ts):
    if "T" in ts:
        date_time = ts.split("T")
        date_str = "-".join([date_time[0][:4], date_time[0]
                             [6:8], date_time[0][4:6]])
        return " ".join([date_str, date_time[1]])
    return ts


def date_partition(ts):
    return str(ts).split(" ")[0]


def events_transformations(sdf):
    udf_transform_timestamp = F.udf(transform_timestamp, "string")
    udf_date_partition = F.udf(date_partition, "string")

    sdf = sdf.withColumnRenamed("desc", "description")
    sdf = sdf.withColumnRenamed("type", "event_type")

    sdf = sdf.withColumn("created", udf_transform_timestamp(
        F.col("utc_timestamp")).cast("string"))

    sdf = sdf.withColumn("ds", udf_date_partition(
        F.col("created")).cast("string")
    )

    sdf = sdf.withColumn("created", sdf["created"].cast("timestamp"))

    # sdf.filter(sdf.created.isNull()).show()
    # sdf.where(sdf.utc_timestamp.contains("T")).show()

    sdf = sdf.select(["id", "device_id", "event_code", "event_detail",
                      "created", "group_id", "description", "event_type", "ds"])
    return sdf


def write_mariadb(sdf, host, user, password, database, table, mode="append"):
    maria_properties = {
        "driver": "org.mariadb.jdbc.Driver",
        "user": user,
        "password": password,
    }
    maria_url = f"jdbc:mysql://{host}:3306/{database}?user={user}&password={password}"

    sdf.write.jdbc(
        url=maria_url, table=table, mode=mode, properties=maria_properties
    )
