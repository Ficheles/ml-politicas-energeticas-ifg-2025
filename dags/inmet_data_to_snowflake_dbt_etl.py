from __future__ import annotations

import pendulum
import os
import re
import boto3
from airflow.models.dag import DAG
from airflow.utils.dates import days_ago

# Using the generic SQL operator for stability
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator 
from airflow.operators.bash import BashOperator 
from airflow.operators.empty import EmptyOperator

# Define S3 and Snowflake parameters
S3_BUCKET = "ml-politicas-energeticas/inmet"
# define years to iterate
YEARS = [2020, 2021, 2022, 2023, 2024, 2025]
SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_DATABASE = "LAB_PIPELINE"
DBT_PROJECT_DIR = "/usr/local/airflow/dags/dbt/inmet_project"
SNOWFLAKE_STAGE = "RAW"  # ⬅️ Correct variable for the Stage Name
SNOWFLAKE_STAGING_TABLE = f"{SNOWFLAKE_STAGE}.STG_INMET_DATA"
FULLY_QUALIFIED_TABLE = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_STAGING_TABLE}"
FULLY_QUALIFIED_FILE_FORMAT = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_STAGE}.INMET_CSV_FORMAT"

with DAG(
    dag_id="inmet_data_to_snowflake_dbt_etl",
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
            SKIP_HEADER = 9
            ENCODING = 'ISO-8859-1' 
            DATE_FORMAT = 'YYYY/MM/DD'
            TIME_FORMAT = 'HH24MI "UTC"'
            REPLACE_INVALID_CHARACTERS = TRUE
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
            NULL_IF = ('', 'NULL', 'NaN');
        """,
    )

    # 2. Load data from S3 to the existing Snowflake Staging Table para cada ano de 2020 a 2025

    # Create a copy task per city inside each year. Cities are executed sequentially within a year,
    # and years are executed sequentially after the previous year's cities finish.
    previous_year_done = None
    # parse S3 bucket and optional prefix (S3_BUCKET may contain a prefix like 'bucket/prefix')
    if "/" in S3_BUCKET:
        bucket_name, initial_prefix = S3_BUCKET.split("/", 1)
    else:
        bucket_name, initial_prefix = S3_BUCKET, ""

    # use the dbt project dir basename as an additional prefix in the bucket path
    project_prefix = os.path.basename(DBT_PROJECT_DIR.strip("/"))

    s3_client = boto3.client("s3")

    for year in YEARS:
        prev_file_task = None

        # build prefix to list objects for this year
        prefix_parts = [p for p in (initial_prefix, project_prefix, str(year)) if p]
        prefix_base = "/".join(prefix_parts)

        # paginate through S3 objects under the prefix
        keys: list[str] = []
        continuation_token = None
        while True:
            list_kwargs = {"Bucket": bucket_name, "Prefix": prefix_base}
            if continuation_token:
                list_kwargs["ContinuationToken"] = continuation_token
            resp = s3_client.list_objects_v2(**list_kwargs)
            for obj in resp.get("Contents", []):
                key = obj.get("Key")
                if not key or key.endswith("/"):
                    continue
                keys.append(key)
            if not resp.get("IsTruncated"):
                break
            continuation_token = resp.get("NextContinuationToken")

        # create a COPY task for each object found
        for key in keys:
            # sanitize a short task id from the key
            safe_name = re.sub(r"[^a-z0-9_]+", "_", os.path.splitext(os.path.basename(key))[0].lower())
            task_id = f"copy_{year}_{safe_name}"
            # avoid duplicate task ids
            if task_id in dag.task_dict:
                copy_task = dag.get_task(task_id)
            else:
                # use the full object key relative to the stage (the stage should be configured to the bucket root)
                s3_file_path = key
                copy_task = SQLExecuteQueryOperator(
                    task_id=task_id,
                    conn_id=SNOWFLAKE_CONN_ID,
                    sql=f"""
                        COPY INTO {FULLY_QUALIFIED_TABLE}
                        FROM (
                            SELECT $1 AS "Data",
                               $2 AS "hora_utc",
                               $3 AS "precipitação_total_horário_(mm)",
                               $4 AS "pressao_atmosferica_ao_nivel_da_estacao_horaria_(m-b)",
                               $5 AS "pressão_atmosferica_maxna_hora_ant_(aut)_(m-b)",
                               $6 AS "pressão_atmosferica_min_na_hora_ant_(aut)_(m-b)",
                               $7 AS "radiacao_global_(kj/m²)",
                               $8 AS "temperatura_do_ar_bulbo_seco_horaria_(°c)",
                               $9 AS "temperatura_do_ponto_de_orvalho_(°c)",
                               $10 AS "temperatura_máxima_na_hora_ant_(aut)_(°c)",
                               $11 AS "temperatura_mínima_na_hora_ant_(aut)_(°c)",
                               $12 AS "temperatura_orvalho_max_na_hora_ant_(aut)_(°c)",
                               $13 AS "temperatura_orvalho_min_na_hora_ant_(aut)_(°c)",
                               $14 AS "umidade_rel_max_na_hora_ant_(aut)_(%)",
                               $15 AS "umidade_rel_min_na_hora_ant_(aut)_(%)",
                               $16 AS "umidade_relativa_do_ar_horaria_(%)",
                               $17 AS "vento_direcao_horaria_(gr)_(°_(gr))",
                               $18 AS "vento_rajada_maxima_(m/s)",
                               $19 AS "vento_velocidade_horaria_(m/s)"
                            FROM @{SNOWFLAKE_DATABASE}.{SNOWFLAKE_STAGE}.STAGE_RAW/{s3_file_path}  (FILE_FORMAT => '{FULLY_QUALIFIED_FILE_FORMAT}')
                        )
                        FILE_FORMAT = (FORMAT_NAME = '{FULLY_QUALIFIED_FILE_FORMAT}')
                        ON_ERROR = 'ABORT_STATEMENT';
                    """,
                )

            # chain tasks: within a year, files run sequentially
            if prev_file_task is None:
                # first file of this year
                if previous_year_done is None:
                    create_file_format >> copy_task
                else:
                    previous_year_done >> copy_task
            else:
                prev_file_task >> copy_task

            prev_file_task = copy_task

        # once all files for the year are created, add a small 'year done' task
        year_done_id = f"year_{year}_done"
        if year_done_id in dag.task_dict:
            year_done = dag.get_task(year_done_id)
        else:
            year_done = EmptyOperator(task_id=year_done_id)
        # last file's task should signal the year's completion
        if prev_file_task is not None:
            prev_file_task >> year_done
        else:
            # if no files found, chain from the file format creation (so the DAG still progresses)
            if previous_year_done is None:
                create_file_format >> year_done
            else:
                previous_year_done >> year_done

        # next year will wait on this
        previous_year_done = year_done
    