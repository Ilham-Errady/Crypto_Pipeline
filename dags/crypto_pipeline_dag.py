import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

SCRIPTS_DIR = "/opt/airflow/scripts_pipeline"

def on_failure_alert(context):
    task_id = context.get('task_instance').task_id
    print(f"🚨 ALERT: Task [{task_id}] failed. Notification sent to Ilham ERRADY!")

default_args = {
    'owner': 'Ilham_ERRADY',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': on_failure_alert
}

with DAG(
    dag_id='cryptopipelinedag',
    default_args=default_args,
    description='End-to-End Crypto Medallion Pipeline via Bash Operators',
    schedule_interval='@daily',
    catchup=False,
    tags=['crypto', 'snowflake', 'production']
) as dag:

    # زدت أمر pip install قبل كل سكريبت باش نضمنوا كاع الـ Dependencies كاينين
    
    ingest_bronze_task = BashOperator(
        task_id='ingestbronze',
        bash_command=f"pip install --user minio requests && python {SCRIPTS_DIR}/ingest_to_bronze.py"
    )

    transform_silver_task = BashOperator(
        task_id='transformsilver',
        bash_command=f"pip install --user pandas pyarrow && python {SCRIPTS_DIR}/transform_to_silver.py"
    )

    build_gold_model_task = BashOperator(
        task_id='buildgoldmodel',
        bash_command=f"pip install --user pandas pyarrow && python {SCRIPTS_DIR}/load_to_gold.py"
    )

    load_snowflake_task = BashOperator(
        task_id='load_snowflake',
        bash_command=f"pip install --user snowflake-connector-python && python {SCRIPTS_DIR}/load_to_warehouse.py"
    )

    ingest_bronze_task >> transform_silver_task >> build_gold_model_task >> load_snowflake_task