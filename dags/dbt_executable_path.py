from cosmos import DbtDag, ProjectConfig, ProfileConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from datetime import datetime

# 1. Configuration for dbt Profile (Unchanged)
profile_config = ProfileConfig(
    profile_name="dbt_inmet_s3_ingestion",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={
            "database": "LAB_PIPELINE",
            "schema": "CORE",
            "warehouse": "LAB_WH_AIRFLOW",
        },
    ),
)

# 2. Configuration for dbt Project Path 
project_config = ProjectConfig(
    # NOTE: Use the absolute path! Assuming 'include/' is at the project root.
    dbt_project_path="/usr/local/airflow/include/dbt_inmet_s3_ingestion",
)

# 3. Instantiate DbtDag with the corrected argument location
my_dbt_dag = DbtDag(
    dag_id="dbt_snowflake_cosmos_dag",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    
    # Pass the main configurations
    project_config=project_config,
    profile_config=profile_config,
    
    # ✅ NEW LOCATION for the dbt executable path is inside operator_args
    operator_args={
        "install_deps": True, 
        "full_refresh": False,
        "dbt_executable_path": "/usr/local/airflow/dbt_venv/bin/dbt", 
    },
)

