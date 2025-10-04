import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import timedelta

# --- Configuration ---
DBT_PROJECT_DIR = "/opt/apps/ml-politicas-energeticas-ifg-2025/dbt/ml_politicas_energeticas"
DBT_PROFILE_DIR = "~/.dbt/"
YEARS_TO_PROCESS = [2023, 2024, 2025]
S3_BUCKET_BASE_URL = "https://ml-politicas-energeticas.s3.us-east-2.amazonaws.com/inmet"

# It's good practice to define default arguments for your DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="dbt_inmet_s3_ingestion",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule_interval="@once",
    catchup=False,
    doc_md="""
    ### DAG de Ingestão de Dados do INMET

    Este DAG executa o modelo dbt 'inmet_data' para cada ano na lista.
    """,
    tags=["dbt", "inmet", "s3"],
)
def dbt_inmet_ingestion_workflow():

    start_task = BashOperator(
        task_id="start",
        bash_command="echo 'Iniciando o processo de ingestão INMET...'"
    )

    end_task = BashOperator(
        task_id="end",
        bash_command="echo 'Processo de ingestão INMET finalizado com sucesso!'"
    )

    for year in YEARS_TO_PROCESS:
        s3_file_path = f"{S3_BUCKET_BASE_URL}/{year}/INMET_CO_GO_A002_GOIANIA_01-01-{year}_A_31-12-{year}.CSV"
        
        dbt_run_task = BashOperator(
            task_id=f"dbt_run_inmet_data_{year}",
            bash_command=f"""
                cd {DBT_PROJECT_DIR} && \
                dbt run --select dados_meteriologicos_inmet --vars '{{"inmet_s3_path": "{s3_file_path}"}}' --profiles-dir {DBT_PROFILE_DIR}
            """,
        )
        
        start_task >> dbt_run_task >> end_task

# This is the standard way to instantiate the DAG object
dbt_inmet_dag = dbt_inmet_ingestion_workflow()