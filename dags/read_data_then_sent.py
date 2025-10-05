from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
import pandas as pd
import io
import logging


def list_and_load_s3_files_to_snowflake():
    bucket_name = "ml-politicas-energeticas"
    prefix = "inmet/2021/"
    table_name = "INMET_DATA_2021"

    # Use IAM Role if aws_default is not configured
    s3_hook = S3Hook(aws_conn_id=None)
    snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    logging.info(f"🔍 Procurando arquivos em: {prefix}")
    files = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)

    if not files:
        logging.warning("⚠️ Nenhum arquivo encontrado no S3.")
        return

    logging.info(f"Encontrados {len(files)} arquivos no prefixo {prefix}")

    for file_key in files:
        if not file_key.endswith(".CSV"):
            continue

        logging.info(f"📥 Lendo arquivo: s3://{bucket_name}/{file_key}")

        # Read raw bytes instead of decoding directly
        obj = s3_hook.get_key(key=file_key, bucket_name=bucket_name)
        raw_bytes = obj.get()["Body"].read()

        # Decode using Latin-1 to handle accented characters
        body = raw_bytes.decode("latin-1")

        # Skip first 8 metadata lines
        df = pd.read_csv(
            io.StringIO(body),
            sep=";",
            decimal=",",
            skiprows=8,
            encoding="latin-1",
        )

        # Normalize columns
        df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]

        logging.info(f"✅ CSV lido com {len(df)} linhas e {len(df.columns)} colunas")

        # Upload DataFrame to Snowflake
        success, nchunks, nrows, _ = snowflake_hook.write_pandas(
            df=df,
            table_name=table_name,
            database="ML_DB",
            schema="PUBLIC",
            chunk_size=16000,
            overwrite=False,
        )

        logging.info(f"📤 Inserido {nrows} registros no Snowflake ({nchunks} chunks).")


default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="s3_to_snowflake_inmet_loader",
    default_args=default_args,
    description="Carrega dados do INMET de S3 para Snowflake",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["inmet", "s3", "snowflake"],
) as dag:

    load_inmet_data = PythonOperator(
        task_id="load_inmet_data",
        python_callable=list_and_load_s3_files_to_snowflake,
    )

    load_inmet_data

