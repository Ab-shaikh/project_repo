# airflow/dags/endtoend_snowpipe_dbt.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.sql import SqlSensor

# DAG default args
default_args = {
    'owner': 'Mariya',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# DAG definition
with DAG(
    dag_id='test_s3_to_snowpipe_dbt',
    default_args=default_args,
    start_date=datetime(2025, 9, 29),
    schedule_interval=None,  # manual trigger
    catchup=False,
) as dag:

    # 1️⃣ Wait for new data in test_table (Snowpipe should load automatically)
    wait_for_new_data = SqlSensor(
        task_id='wait_for_new_data',
        conn_id='snowflake_default',       # your Snowflake connection in Airflow
        sql="SELECT COUNT(*) FROM test_table WHERE created_at > DATEADD(minute, -10, CURRENT_TIMESTAMP);",
        poke_interval=60,                  # check every 60 seconds
        timeout=600,                        # wait max 10 minutes
    )

    # 2️⃣ Run dbt incremental model
    run_dbt_incremental = BashOperator(
        task_id='run_dbt_incremental',
        bash_command="""
        cd /opt/dbt
        dbt run --models staging.stg_test_table
        """,
    )

    wait_for_new_data >> run_dbt_incremental
