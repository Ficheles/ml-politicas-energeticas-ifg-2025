from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import zipfile
import os
import boto3
import io # Crucial for in-memory streaming
import logging

# --- Config and Constants ---
BASE_URL = "https://portal.inmet.gov.br/uploads/dadoshistoricos/"
S3_BUCKET = "ml-politicas-energeticas"

# Set up logging
logger = logging.getLogger("airflow.task")

# Function to get the list of years dynamically
def get_years_to_process():
    """Generates the list of years from 2000 up to the current year (2025)."""
    current_year = datetime.now().year
    # NOTE: Set the end year explicitly to the current time context (2025)
    # If this DAG were run in 2026, it would include 2026.
    return list(range(2000, 2025 + 1)) 

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="inmet_csv_to_s3_yearly_stream_in_memory",
    default_args=default_args,
    description="Download, unzip, and stream INMET CSV data to S3, entirely in-memory.",
    schedule_interval="@once",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["inmet", "s3", "csv", "in-memory"],
)
def inmet_csv_to_s3_in_memory():

    @task
    def generate_year_list():
        """Returns the list of years for dynamic task mapping."""
        return get_years_to_process()

    @task
    def stream_zip_to_s3(year: int):
        """
        1. Downloads the zip file directly into a BytesIO object (in memory).
        2. Iterates over files inside the zip (still in memory).
        3. Streams each file's content directly to S3 without local disk writes.
        """
        
        url = f"{BASE_URL}{year}.zip"
        s3_prefix = f"inmet/{year}/"
        s3_client = boto3.client("s3")

        logger.info(f"--- Starting in-memory stream processing for year: {year} ---")
        
        # 1. Stream Download into a BytesIO buffer
        logger.info(f"Downloading ZIP data for {year} into memory from {url}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        zip_buffer = io.BytesIO()
        for chunk in response.iter_content(chunk_size=8192):
            zip_buffer.write(chunk)
        zip_buffer.seek(0) # Rewind the buffer to the beginning
        
        # 2. Open the ZIP file from the in-memory buffer
        with zipfile.ZipFile(zip_buffer, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            
            logger.info(f"Found {len(file_list)} files in the in-memory zip.")
            
            # 3. Stream each file to S3
            for file_name in file_list:
                # Skip directories and empty file names
                if not file_name or file_name.endswith('/'):
                    continue

                # Open the file inside the zip in memory
                with zip_ref.open(file_name, 'r') as zip_file:
                    
                    # Read the content into another in-memory buffer
                    file_content_bytes = zip_file.read()
                    
                    # Create a file-like object from the bytes for S3 upload
                    upload_file_obj = io.BytesIO(file_content_bytes)
                    
                    s3_key = f"{s3_prefix}{file_name}"

                    # Use upload_fileobj for streaming the in-memory buffer to S3
                    s3_client.upload_fileobj(
                        upload_file_obj, 
                        S3_BUCKET, 
                        s3_key
                    )
                    
                    # Log success and memory usage (approx)
                    size_mb = len(file_content_bytes) / (1024 * 1024)
                    logger.info(f"Uploaded {file_name} ({size_mb:.2f} MB) to s3://{S3_BUCKET}/{s3_key}")
                    
        # Explicitly clear the large buffer (Python GC will clean up, but this helps)
        del zip_buffer
        
        logger.info(f"--- Finished in-memory processing for year: {year} ---")
        
        return f"Successfully processed year {year}"


    # --- Task Flow ---
    
    # 1. Generate the list of years (e.g., [2000, 2001, ..., 2025])
    years = generate_year_list()
    
    # 2. Dynamically map the single, consolidated stream task over the list of years
    stream_zip_to_s3.expand(year=years)


dag = inmet_csv_to_s3_in_memory()