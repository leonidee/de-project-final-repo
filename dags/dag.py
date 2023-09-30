from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator 
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.decorators import dag, task  
from airflow.models.baseoperator import chain 
import click
import dotenv
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

dotenv.load_dotenv()
import getpass

# package
sys.path.append(str(Path(__file__).parent.parent))
import os

@dag(
    dag_id="testing-dag",
    schedule=None,
    start_date=datetime(2023, 9, 30),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["SPARK"],
    default_args={
        "owner": "@leonidgrishenkov",
    },
    default_view="grid",
)
def taskflow() -> ...:

    begin = EmptyOperator(task_id="begining")

    spark_job = SparkSubmitOperator(
        task_id='spark_submit_job',
        application='/app/src/transaction_service_stream_collector/runner.py',  # It's a path inside of your Spark cluster  
        name='airflow-spark',  # Name of your application
        conn_id='spark_default',  # Your Spark connection
        # conf={'master': 'spark://158.160.64.46:7077'},  # Make sure you point to your spark master, ensure 
    )

    # test_job = BashOperator(
    #     task_id='test_job',
    #     bash_command="sudo docker exec -it spark-master /bin/bash",
    #     # bash_command="/opt/bitnami/spark/bin/spark-submit --verbose --deploy-mode client --master spark://spark-master:7077 /app/src/transaction_service_stream_collector/runner.py  --mode=DEV --log-level=WARN", 
    # )

    end = EmptyOperator(task_id="ending")

    chain(begin, end)

taskflow()