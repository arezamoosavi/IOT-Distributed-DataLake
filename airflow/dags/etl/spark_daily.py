import os
import sys
import logging

from common import get_spark_session, events_transformations, write_mariadb

from delta.tables import DeltaTable


# spark session
spark = get_spark_session("ETL", "thrift://hive:9083", "http://minio:9000",
                          "spark", "spark12345")
# Set log4j
spark.sparkContext.setLogLevel("ERROR")
log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger("LOGGER")
logger.setLevel(log4jLogger.Level.INFO)


def create_daily_app(input_path="s3a://datalake/batch/*/*.json",
                     output_path="s3a://datalake/deltatables/events/", **kwargs):

    sdf = spark.read.json(input_path)
    sdf = events_transformations(sdf)
    sdf.printSchema()

    delta_table = DeltaTable.forPath(spark, output_path)
    delta_table.alias("t1").merge(
        sdf.alias("t2"),
        "t1.id = t2.id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    write_mariadb(sdf, "mariadb", "root", "root", "dwh", "main_events")

    spark.stop()


if __name__ == "__main__":

    input_path = str(sys.argv[1])
    output_path = str(sys.argv[2])

    create_daily_app(input_path, output_path)
