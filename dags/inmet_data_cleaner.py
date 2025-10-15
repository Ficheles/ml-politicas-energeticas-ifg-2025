from airflow.decorators import dag, task
from datetime import datetime, timedelta
import boto3

# Config
S3_BUCKET = "ml-politicas-energeticas"
S3_PREFIX = "inmet/"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="inmet_data_cleaner",
    default_args=default_args,
    description="Clean S3 bucket, leaving only CSVs containing GO",
    schedule_interval="@once",  # adjust if you want periodic runs
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["s3", "clean", "go"],
)
def clean_s3_go_only():

    @task
    def clean_s3_bucket():
        s3 = boto3.client("s3")

        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                filename = key.split("/")[-1]
                # Keep only files containing 'GO'
                if "GO" not in filename:
                    print(f"Deleting {key} from S3")
                    s3.delete_object(Bucket=S3_BUCKET, Key=key)
                else:
                    print(f"Keeping {key}")

    clean_s3_bucket()


dag = clean_s3_go_only()
