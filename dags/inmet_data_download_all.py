from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import zipfile
import os
import boto3

# Config
URL = "https://portal.inmet.gov.br/uploads/dadoshistoricos/2024.zip"
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
    dag_id="inmet_data_download_all",
    default_args=default_args,
    description="Download INMET CSV, unzip, and upload to S3 (decorators version)",
    schedule_interval="@once",  # adjust if you want periodic runs
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["inmet", "s3", "csv"],
)
def inmet_data_download_all():

    @task
    def get_years():
        current_year = datetime.now().year
        return list(range(2000, current_year + 1))

    @task
    def download_file(year):
        zip = LOCAL_ZIP + year + ".zip"
        response = requests.get(URL, stream=True)
        response.raise_for_status()
        with open(zip, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return zip

    @task
    def unzip_file(local_zip: str):
        os.makedirs(EXTRACT_PATH, exist_ok=True)
        with zipfile.ZipFile(local_zip, 'r') as zip_ref:
            zip_ref.extractall(EXTRACT_PATH)
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
    def process_all():
        years = get_years()

        for year in years:
            dl = download_file(year)
            unzip = unzip_file(dl)
            upload = upload_to_s3(year, unzip)

            # Chain tasks using >>
            dl >> unzip >> upload   

    process_all

dag = inmet_data_download_all()

