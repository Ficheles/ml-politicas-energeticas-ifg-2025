from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import zipfile
import io
import boto3

# Config
URL = "https://portal.inmet.gov.br/uploads/dadoshistoricos/"
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
    dag_id="inmet_csv_to_s3_streaming",
    default_args=default_args,
    description="Download ZIP, extract CSV in memory, and upload to S3",
    schedule_interval="@once",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["inmet", "s3", "csv"],
    concurrency=4,  # 4 years at a time
)
def inmet_csv_to_s3():

    @task
    def download_extract_upload(year: int):
        url = f"{URL}{year}.zip"
        s3 = boto3.client("s3")

        # Stream ZIP into memory
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with io.BytesIO() as zip_buffer:
            for chunk in response.iter_content(chunk_size=8192):
                zip_buffer.write(chunk)
            zip_buffer.seek(0)

            # Open ZIP in memory
            with zipfile.ZipFile(zip_buffer) as zip_ref:
                for file_info in zip_ref.infolist():
                    # Only process CSV files
                    if file_info.filename.endswith(".csv"):
                        with zip_ref.open(file_info) as csv_file:
                            s3_key = f"{S3_PREFIX}{year}/{file_info.filename}"
                            s3.upload_fileobj(csv_file, S3_BUCKET, s3_key)
                            print(f"Uploaded {s3_key}")

    years = list(range(2000, datetime.now().year + 1))
    for year in years:
        download_extract_upload(year)

dag = inmet_csv_to_s3()

