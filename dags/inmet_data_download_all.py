from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import zipfile
import os
import boto3

# Config
URL = "https://portal.inmet.gov.br/uploads/dadoshistoricos/"
LOCAL_ZIP = "/tmp/"
EXTRACT_PATH = "/tmp/inmet_data"
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
    dag_id="inmet_csv_to_s3_all_years_manual",
    default_args=default_args,
    description="Download INMET CSV, unzip, and upload to S3",
    schedule_interval="@once",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["inmet", "s3", "csv"],
    max_active_tasks=4, 
    max_active_tasks_per_dag=4,
)
def inmet_csv_to_s3():

    @task
    def download_file(year):
        zip_path = os.path.join(LOCAL_ZIP, f"{year}.zip")
        url = f"{URL}{year}.zip"
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Downloaded {zip_path}")
        return zip_path

    @task
    def unzip_file(local_zip: str):
        os.makedirs(EXTRACT_PATH, exist_ok=True)
        with zipfile.ZipFile(local_zip, 'r') as zip_ref:
            zip_ref.extractall(EXTRACT_PATH)
        print(f"Unzipped {local_zip} to {EXTRACT_PATH}")
        return EXTRACT_PATH

    @task
    def upload_to_s3(year, folder: str):
        s3 = boto3.client("s3")
        for root, dirs, files in os.walk(folder):
            for file in files:
                local_path = os.path.join(root, file)
                s3_key = f"{S3_PREFIX}{year}/{file}"
                s3.upload_file(local_path, S3_BUCKET, s3_key)
                print(f"Uploaded {local_path} to s3://{S3_BUCKET}/{s3_key}")

    @task
    def cleanup_tmp(year):
        import shutil
        zip_path = os.path.join(LOCAL_ZIP, f"{year}.zip")
        folder_path = EXTRACT_PATH
        # Remove the ZIP
        if os.path.exists(zip_path):
            os.remove(zip_path)
            print(f"Deleted {zip_path}")
        # Remove extracted folder
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)
            print(f"Deleted {folder_path}")

    # Generate years directly in DAG context
    years = list(range(2000, datetime.now().year + 1))

    # Loop over years and chain tasks
    for year in years:
        dl = download_file(year)
        unzip = unzip_file(dl)
        upload = upload_to_s3(year, unzip)
        cleanup = cleanup_tmp(year)

        # Chain tasks
        dl >> unzip >> upload >> cleanup


dag = inmet_csv_to_s3()
