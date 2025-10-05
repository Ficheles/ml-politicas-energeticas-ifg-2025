from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.utils.dates import days_ago

# Using the generic SQL operator for stability
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator 
from airflow.operators.bash import BashOperator 

# Define S3 and Snowflake parameters
S3_BUCKET = "ml-politicas-energeticas/inmet"
S3_FILE_PATH = "2021/INMET_CO_GO_A002_GOIANIA_01-01-2021_A_31-12-2021.CSV"
SNOWFLAKE_CONN_ID = "snowflake_default" 
SNOWFLAKE_DATABASE = "LAB_PIPELINE" 
DBT_PROJECT_DIR = "/usr/local/airflow/dags/dbt/inmet_project" 
SNOWFLAKE_STAGE = "RAW" # ⬅️ Correct variable for the Stage Name
SNOWFLAKE_STAGING_TABLE = f"{SNOWFLAKE_STAGE}.STG_INMET_DATA"
FULLY_QUALIFIED_TABLE = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_STAGING_TABLE}"
FULLY_QUALIFIED_FILE_FORMAT = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_STAGE}.INMET_CSV_FORMAT"

with DAG(
    dag_id="inmet_data_to_snowflake_dbt_etl-2",
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    tags=["elt", "snowflake", "dbt", "s3"],
) as dag:
    
    # 1. Create a Snowflake File Format in the RAW schema
    create_file_format = SQLExecuteQueryOperator(
        task_id="create_csv_file_format",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            CREATE OR REPLACE FILE FORMAT {FULLY_QUALIFIED_FILE_FORMAT}
            TYPE = CSV
            FIELD_DELIMITER = ';'
            SKIP_HEADER = 8
            ENCODING = 'ISO-8859-1' 
            DATE_FORMAT = 'YYYY/MM/DD'
            TIME_FORMAT = 'HH24MI "UTC"'
            REPLACE_INVALID_CHARACTERS = TRUE
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
            NULL_IF = ('', 'NULL', 'NaN');
        """,
    )

    # 2. Load data from S3 to the existing Snowflake Staging Table
    copy_into_staging = SQLExecuteQueryOperator(
        task_id="copy_s3_to_snowflake_staging",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            -- Copy a single CSV file from the named Snowflake stage into the target table.
            -- Ensure SNOWFLAKE_STAGE was created and points to the S3 bucket.
            COPY INTO {FULLY_QUALIFIED_TABLE} 
            FROM (
                SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19
                FROM @{SNOWFLAKE_DATABASE}.{SNOWFLAKE_STAGE}.STAGE_RAW/{S3_FILE_PATH}  (FILE_FORMAT => '{FULLY_QUALIFIED_FILE_FORMAT}')
            )
            ON_ERROR = 'ABORT_STATEMENT';
        """,
    )


    # # 3. Run the dbt models for transformation using the BashOperator
    # run_dbt_model = BashOperator(
    #     task_id="run_dbt_models",
    #     bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --models ft_hourly_weather --target prod",
    # )

    # Define task dependencies
    (
        create_file_format
        >> copy_into_staging
        # >> run_dbt_model
    )