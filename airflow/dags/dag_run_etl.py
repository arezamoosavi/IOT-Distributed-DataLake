from __future__ import print_function

from datetime import datetime

import airflow
from airflow.operators.bash_operator import BashOperator

args = {
    "owner": "airflow",
    "provide_context": True,
    "catchup": False,
}

# -------------------Initial-------------------
initial_dag = airflow.DAG(
    dag_id="initial_load_events_delta",
    default_args=args,
    start_date=datetime(year=2021, month=9, day=23),
    schedule_interval=None,
    max_active_runs=1,
    concurrency=1,
)

task_initial_dag_spark_etl = BashOperator(
    task_id="deltaTable_initial_load",
    bash_command="""spark-submit --master spark://spark-master:7077 \
        --deploy-mode client --driver-memory 2g --num-executors 2 \
            --packages io.delta:delta-core_2.12:1.0.0 --py-files dags/utils/common.py \
                --jars dags/jars/aws-java-sdk-1.11.534.jar,dags/jars/aws-java-sdk-bundle-1.11.874.jar,dags/jars/delta-core_2.12-1.0.0.jar,dags/jars/hadoop-aws-3.2.0.jar,dags/jars/mariadb-java-client-2.7.4.jar \
                    dags/etl/spark_initial_load.py""",
    dag=initial_dag,
)

task_initial_dag_hive_tables = BashOperator(
    task_id="deltaTable_hive_table",
    bash_command="""spark-submit --master spark://spark-master:7077 \
        --deploy-mode client --driver-memory 2g --num-executors 2 \
            --packages io.delta:delta-core_2.12:1.0.0 --py-files dags/utils/common.py \
                --jars dags/jars/aws-java-sdk-1.11.534.jar,dags/jars/aws-java-sdk-bundle-1.11.874.jar,dags/jars/delta-core_2.12-1.0.0.jar,dags/jars/hadoop-aws-3.2.0.jar,dags/jars/mariadb-java-client-2.7.4.jar \
                    dags/etl/spark_create_tables.py""",
    dag=initial_dag,
)

# -------------------Daily-------------------
daily_dag = airflow.DAG(
    dag_id="daily_upsert_events_delta",
    default_args=args,
    start_date=datetime(year=2021, month=9, day=23),
    schedule_interval=None,
    max_active_runs=1,
    concurrency=1,
)

task_daily_dag_spark_etl = BashOperator(
    task_id="deltaTable_upsert",
    bash_command="""spark-submit --master spark://spark-master:7077 \
        --deploy-mode client --driver-memory 2g --num-executors 2 \
            --packages io.delta:delta-core_2.12:1.0.0 --py-files dags/utils/common.py \
                --jars dags/jars/aws-java-sdk-1.11.534.jar,dags/jars/aws-java-sdk-bundle-1.11.874.jar,dags/jars/delta-core_2.12-1.0.0.jar,dags/jars/hadoop-aws-3.2.0.jar,dags/jars/mariadb-java-client-2.7.4.jar \
                    dags/etl/spark_daily.py""",
    dag=daily_dag,
)


task_initial_dag_spark_etl >> task_initial_dag_hive_tables
task_daily_dag_spark_etl
