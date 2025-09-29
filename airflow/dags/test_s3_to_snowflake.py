from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="test_s3_to_snowflake",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # 1️⃣ List files in stage (just a test)
    list_stage_files = SnowflakeOperator(
        task_id="list_stage_files",
        snowflake_conn_id="snowflake_default",  # must match Airflow connection ID
        sql="""
        LIST @s3_stage;
        """
    )

    # 2️⃣ Optional: Copy file into table for testing
    copy_file = SnowflakeOperator(
        task_id="copy_csv_to_table",
        snowflake_conn_id="snowflake_default",
        sql="""
        CREATE OR REPLACE TABLE test_table (
            id STRING,
            name STRING
        );

        COPY INTO test_table
        FROM @s3_stage/test_file.csv
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
        """
    )

    list_stage_files >> copy_file
