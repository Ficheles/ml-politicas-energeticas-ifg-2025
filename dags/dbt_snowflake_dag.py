from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define the absolute path to your dbt project/profiles
DBT_PROJECT_PATH = "/usr/local/airflow/include/dbt_inmet_s3_ingestion"

with DAG(
    dag_id="dbt_debug_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['dbt', 'snowflake', 'troubleshooting']
) as dag:
    
    # Task 1: Search for profiles.yml
    # This task will run first to find the exact path of the profiles.yml file
    # since all hardcoded paths have failed so far. We are searching common 
    # mount points and user directories.
    search_profile = BashOperator(
        task_id="search_profiles_file",
        bash_command=(
            "echo '--- Searching for profiles.yml in common Airflow/dbt locations ---' && "
            "find /usr/local/airflow /home /root /opt -name 'profiles.yml' || "
            "echo 'profiles.yml not found in common locations.' "
        )
    )

    # Task 2: dbt debug (still using the old path for now)
    # Once the 'search_profiles_file' task runs, look at its logs for the correct path.
    # You will then replace '/root/.dbt' with the path found in the logs.
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=(
            f"cd /tmp && "
            f"dbt debug --project-dir {DBT_PROJECT_PATH} --profiles-dir /root/.dbt"
        )
    )
    
    # Ensure the search runs before the debug task
    search_profile >> dbt_debug

