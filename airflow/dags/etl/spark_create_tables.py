import os
import sys
import logging

from delta.tables import DeltaTable

from common import get_spark_session


# spark session
spark = get_spark_session("ETL", "thrift://hive:9083", "http://minio:9000",
                          "spark", "spark12345")
# Set log4j
spark.sparkContext.setLogLevel("ERROR")
log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger("LOGGER")
logger.setLevel(log4jLogger.Level.INFO)

delta_table = DeltaTable.forPath(spark, "s3a://datalake/deltatables/events/")
delta_table.generate("symlink_format_manifest")

sql_delta_table = """CREATE EXTERNAL TABLE IF NOT EXISTS hive_events
(id string, device_id string, event_code string, event_detail string,
time timestamp, group_id string, desc string, type string, ds string)
PARTITIONED BY (ds)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3a://datalake/deltatables/events/_symlink_format_manifest/'"""
spark.sql(sql_delta_table)

spark.sql("MSCK REPAIR TABLE hive_events")
spark.sql("ALTER TABLE delta.`s3a://datalake/deltatables/events/` SET TBLPROPERTIES(delta.compatibility.symlinkFormatManifest.enabled=true)")
spark.sql("CREATE TABLE IF NOT EXISTS delta_events USING DELTA LOCATION 's3a://datalake/deltatables/events/'")

spark.stop()
