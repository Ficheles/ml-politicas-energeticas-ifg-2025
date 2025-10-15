from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import zipfile
import os
import boto3

# Config
URL = "https://portal.inmet.gov.br/uploads/dadoshistoricos/2021.zip"
LOCAL_ZIP = "/tmp/2021.zip"
EXTRACT_PATH = "/tmp/inmet_data"
S3_BUCKET = "ml-politicas-energeticas"
S3_PREFIX = "inmet/2021/"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="inmet_csv_to_s3",
    default_args=default_args,
    description="Download INMET CSV, unzip, and upload to S3 (decorators version)",
    schedule_interval="@once",  # adjust if you want periodic runs
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["inmet", "s3", "csv"],
)
def inmet_csv_to_s3():

    @task
    def download_file():
        response = requests.get(URL, stream=True)
        response.raise_for_status()
        with open(LOCAL_ZIP, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return LOCAL_ZIP

    @task
    def unzip_file(local_zip: str):
        os.makedirs(EXTRACT_PATH, exist_ok=True)
        with zipfile.ZipFile(local_zip, 'r') as zip_ref:
            zip_ref.extractall(EXTRACT_PATH)
        return EXTRACT_PATH

    @task
    def upload_to_s3(folder: str):
        s3 = boto3.client("s3")
        for root, dirs, files in os.walk(folder):
            for file in files:
                local_path = os.path.join(root, file)
                s3_key = f"{S3_PREFIX}{file}"
                s3.upload_file(local_path, S3_BUCKET, s3_key)
                print(f"Uploaded {local_path} to s3://{S3_BUCKET}/{s3_key}")

    # Task flow
    local_zip = download_file()
    folder = unzip_file(local_zip)
    upload_to_s3(folder)


dag = inmet_csv_to_s3()
