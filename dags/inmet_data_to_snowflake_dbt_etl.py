from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.utils.dates import days_ago

# Using the generic SQL operator for stability
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator 
from airflow.operators.bash import BashOperator 
from airflow.operators.empty import EmptyOperator

# Define S3 and Snowflake parameters
S3_BUCKET = "ml-politicas-energeticas/inmet"
# define years and cities to iterate
YEARS = [2020, 2021, 2022, 2023, 2024, 2025]
CITIES = ["GOIANIA", "INHUMAS", "TRINDADE"]
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
    for year in YEARS:
        prev_city_task = None
        for city in CITIES:
            s3_file_path = f"{year}/INMET_CO_GO_A002_{city}_01-01-{year}_A_31-12-{year}.CSV"
            task_id = f"copy_{year}_{city}".lower()
            if task_id in dag.task_dict:
                copy_task = dag.get_task(task_id)
            else:
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

            # chain tasks: within a year, cities run sequentially
            if prev_city_task is None:
                # first city of this year
                if previous_year_done is None:
                    create_file_format >> copy_task
                else:
                    previous_year_done >> copy_task
            else:
                prev_city_task >> copy_task

            prev_city_task = copy_task
            # once all cities for the year are created, add a small 'year done' task
            year_done_id = f"year_{year}_done"
            if year_done_id in dag.task_dict:
                year_done = dag.get_task(year_done_id)
            else:
                year_done = EmptyOperator(task_id=year_done_id)
            # last city's task should signal the year's completion
            prev_city_task >> year_done
            # next year will wait on this
            previous_year_done = year_done
    