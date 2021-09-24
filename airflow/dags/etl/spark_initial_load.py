import os
import sys
import logging

from common import get_spark_session, events_transformations, write_mariadb


# spark session
spark = get_spark_session("ETL", "thrift://hive:9083", "http://minio:9000",
                          "spark", "spark12345")
# Set log4j
spark.sparkContext.setLogLevel("ERROR")
log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger("LOGGER")
logger.setLevel(log4jLogger.Level.INFO)

sdf = spark.read.json("s3a://datalake/batch/1/*.json")
sdf = events_transformations(sdf)
sdf.printSchema()

sdf.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy(
    "ds").save('s3a://datalake/deltatables/events/')

write_mariadb(sdf, "mariadb", "root", "root", "dwh", "main_events")

spark.stop()
