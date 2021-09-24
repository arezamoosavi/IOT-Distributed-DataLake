from __future__ import print_function

from datetime import datetime

import airflow
from airflow.operators.bash_operator import BashOperator

args = {
    "owner": "airflow",
    "provide_context": True,
    "catchup": False,
}

dag = airflow.DAG(
    dag_id="initial_load",
    default_args=args,
    start_date=datetime(year=2021, month=9, day=19),
    schedule_interval="@once",
    max_active_runs=1,
    concurrency=1,
)

task_spark_etl = BashOperator(
    task_id="spark_initial_load",
    bash_command="spark-submit "
    "--master local "
    "--deploy-mode client "
    "--driver-memory 2g "
    "--jars {{var.value.airflow_home}}/dags/catfish_dwh/jars/postgresql-42.2.5.jar "
    "--py-files {{var.value.airflow_home}}/dags/catfish_dwh/utils/connector.py,"
    "{{var.value.airflow_home}}/dags/utils/common.py,"
    "{{var.value.airflow_home}}/dags/etl/spark_app.py ",
    dag=dag,
)

start_task >> task_spark_etl
