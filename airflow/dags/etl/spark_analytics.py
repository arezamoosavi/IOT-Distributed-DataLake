import os
import sys
import logging

from common import get_spark_session

# spark session
spark = get_spark_session("ETL", "thrift://hive:9083", "http://minio:9000",
                          "spark", "spark12345")
# Set log4j
spark.sparkContext.setLogLevel("ERROR")
log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger("LOGGER")
logger.setLevel(log4jLogger.Level.INFO)

spark.sql(
    """SELECT event_type, COUNT(event_type) AS c_event_type FROM delta_events 
    group by event_type""").show()

spark.sql(
    """SELECT COUNT(*) AS count FROM delta_events WHERE event_type = 'T2'""").show()

spark.stop()
