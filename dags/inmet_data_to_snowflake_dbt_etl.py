from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook 
from airflow.providers.ssh.operators.ssh import SSHOperator 
import pandas as pd
import io
import logging

# ✅ NEW IMPORT: Import the specific tool from the Snowflake connector library
from snowflake.connector.pandas_tools import write_pandas 

# Define parameters
S3_BUCKET = "ml-politicas-energeticas"
S3_KEY = "inmet/2021/INMET_CO_GO_A002_GOIANIA_01-01-2021_A_31-12-2021.CSV"
SNOWFLAKE_CONN_ID = "snowflake_default" 
SNOWFLAKE_DATABASE = "LAB_PIPELINE" 
SNOWFLAKE_STAGING_TABLE = "RAW.STG_INMET_DATA" 
FULLY_QUALIFIED_TABLE = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_STAGING_TABLE}"
DBT_PROFILE_NAME = "dbt_inmet_s3_ingestion"
DBT_VENV_PATH = "/usr/local/airflow/dbt_venv/bin/dbt"
DBT_PROJECT_DIR = "/usr/local/airflow/include/dbt_inmet_s3_ingestion" 
AWS_CONN_ID = "aws_default" 

# --- ETL LOGIC (Executed by PythonOperator) ---

def read_s3_and_load_snowflake(
    s3_bucket: str, s3_key: str, table_name: str, conn_id: str
):
    logging.info(f"Starting ETL for s3://{s3_bucket}/{s3_key}")
    
    # 1. Extraction: Download file content as raw bytes and decode
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3_client = s3_hook.get_conn()
    obj = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    file_bytes = obj['Body'].read()
    file_content = file_bytes.decode('latin-1') 
    file_io = io.StringIO(file_content)

    # 2. Transformation: Read, parse, and prepare for load
    df = pd.read_csv(
        file_io,
        sep=";",
        skiprows=8,
        engine="python", 
        header=None 
    )

    df.dropna(axis=1, how='all', inplace=True) 
    
    # Data Preparation: Create the RAW_DATA VARIANT column (list/array)
    df['RAW_DATA'] = df.apply(lambda row: row.tolist(), axis=1)
    df_load = df[['RAW_DATA']]
    logging.info(f"DataFrame prepared with {len(df_load)} rows for loading.")

    # 3. Load: Use SnowflakeHook and external write_pandas function
    snowflake_hook = SnowflakeHook(snowflake_conn_id=conn_id)
    
    # Truncate the staging table to ensure idempotency
    snowflake_hook.run(f"TRUNCATE TABLE {table_name}")
    
    # ✅ FIX APPLIED HERE: Call write_pandas directly from the imported function
    success, nchunks, nrows, output = write_pandas(
        conn=snowflake_hook.get_conn(), # Pass the connection object
        df=df_load, 
        table_name=table_name.split('.')[-1], 
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_STAGING_TABLE.split('.')[0], 
        quote_identifiers=False # Ensure column names aren't quoted
    )
    
    if not success:
        logging.error(f"Snowflake write_pandas failed: {output}")
        raise Exception("Failed to load data into Snowflake.")

    logging.info(f"Data successfully loaded into {table_name} (Rows: {nrows}).")


# --- DAG DEFINITION ---

with DAG(
    dag_id="inmet_data_etl_python",
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    tags=["etl", "snowflake", "dbt", "python"],
) as dag:
    
    load_csv_to_snowflake = PythonOperator(
        task_id="load_csv_to_snowflake",
        python_callable=read_s3_and_load_snowflake,
        op_kwargs={
            "s3_bucket": S3_BUCKET,
            "s3_key": S3_KEY,
            "table_name": FULLY_QUALIFIED_TABLE,
            "conn_id": SNOWFLAKE_CONN_ID,
        },
    )

    # run_dbt_model = BashOperator(
    #     task_id="run_dbt_models",
    #     bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --models ft_hourly_weather --target prod",
    # )

    # run_dbt_model = BashOperator(
    #         task_id="dbt_debug",
    #         bash_command=(
    #             f"cd /tmp && "
    #             f"dbt debug --project-dir {DBT_PROJECT_DIR} --profiles-dir /root/.dbt"
    #         )
    #     )

    run_dbt_model = SSHOperator(
        task_id="dbt_run_transformations",
        ssh_conn_id=SSH_CONN_ID,
        # The command sequence executes on the remote host machine:
        # 1. Activate the dbt virtual environment.
        # 2. Change directory to the dbt project folder.
        # 3. Execute dbt run, which must be configured on the host to use the Snowflake database.
        command=(
            f"{DBT_VENV_ACTIVATE} && "
            f"cd {DBT_HOST_DIR} && "
            f"dbt run --target dev"
        ),
        # You may need to pass the dbt profile directory if it's not the default:
        # command=(f"dbt run --profiles-dir /home/user/.dbt --target dev"), 
    )

    load_csv_to_snowflake >> run_dbt_model