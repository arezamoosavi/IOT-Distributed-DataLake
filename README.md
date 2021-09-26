# Introduction
This is a demo project of datalake and data warehouse for IOT event dataset. There is raw data in json format and it is going to be extracted, transformed and persisted into delta for datalake and data warehouse.
## Technologies
The main technologies are:
* Python3
* Docker
* Spark
* Airflow
* MinIo
* DeltaTable
* Hive
* Mariadb
* Presto
* Superset

## How is it work?
I this section each part of this ETL pipeline will be illustrated:
### Spark
Spark is used to read and write data in distributed and scalable manner.
```bash
make spark
```
will run spark master and one instance of worker
```bash
make scale-spark
```
will scale spark worker.
### Airflow
One of the best workflow managemnet for spark jobs.
```bash
make airflow
```
### MinIo
An opensource, distributed and performant object storage for datalake files and hive tables.
```bash
make minio
```
### DeltaTable (Deltalake)
An opensource columnar data parquet files with snappy compression. Delta supports update and delete, whicj is very nice. All the necessary jar files are added to hive and spark dockers.
### Hive and Mariadb
In order to create tables to run Spark SQL on delta tables, spark needs hive metastore and hive needs mariadb as metastoreDb. Mariadb is also used for data warehouse for to run query faster and create dashboards.
```bash
make hive
```
It will create hive and mariadb instances and in order to create tables:
```bash
make exec-mariadb
```
```sql
CREATE DATABASE dwh;

USE dwh;

CREATE TABLE IF NOT EXISTS main_events (id VARCHAR(255) NOT NULL, 
device_id VARCHAR(255), event_code VARCHAR(255), event_detail VARCHAR(255), 
created TIMESTAMP, group_id VARCHAR(255), description VARCHAR(255), 
event_type VARCHAR(255), ds VARCHAR(255)) ENGINE=COLUMNSTORE;
```
### Presto
In order to have acces to delta tables without spark, presto is going to be employed as distributed query engine. It works with superset and hive tables. Presto is opensource, scaleable and can connect to any databases.
```bash
make presto-cluster
```
This with create a presto coordinator and worker, the worker can scale horizontally. In order to query data tables:
```bash
make presto-cli
```
In presto-cli just like spark sql, any query can be run.
### Superset
Superset is opensource, supports any database with many dashbord styles and ver famous to create dashboards or to get hands on databases.