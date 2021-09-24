import os
import sys
import logging

from pyspark.sql import functions as F
from common import get_spark_session, write_mariadb

# spark session
spark = get_spark_session("ETL", "thrift://hive:9083", "http://minio:9000",
                          "spark", "spark12345")
# Set log4j
spark.sparkContext.setLogLevel("ERROR")
log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger("LOGGER")
logger.setLevel(log4jLogger.Level.INFO)

# sdf = spark.read.format("delta").load(
#     "s3a://datalake/deltatables/events/")
# sdf.createOrReplaceTempView("temp_sdf")
# sdf.createGlobalTempView("g_temp_sdf")

resultdf = spark.sql(
    """SELECT ds, COUNT(ds) AS event_counts FROM delta_events group by ds""")

resultdf = resultdf.withColumn("ds", F.to_date(F.col("ds"), 'yyyy-MM-dd'))

write_mariadb(resultdf, "mariadb", "root", "root", "dwh", "daily_events")

spark.stop()
