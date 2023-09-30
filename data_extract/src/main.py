import gzip
import os
import requests
import shutil
import ssl
import zipfile

from util import bq_schema

from dotenv import load_dotenv
from google.cloud import storage, bigquery


# TODO: Port this to support requests, not urllib
def get_ssl_context() -> ssl.SSLContext:
    context: ssl.SSLContext = ssl.create_default_context()
    context.set_ciphers("HIGH:!DH:!aNULL")
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE

    return context


def download_file(year: int, month: int, target_dir: str) -> str:
    base_url: str = "https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present"
    base_filename: str = f"{year}_{month}.zip"

    target_url: str = f"{base_url}_{base_filename}"
    target_filename: str = f"{target_dir}/{base_filename}"

    env: str = os.getenv("ENV", "DEV")
    verify_ssl: bool = env == "PROD"

    if not os.path.exists(target_dir):
        os.mkdir(target_dir)

    with open(file=target_filename, mode="wb") as target_file:
        print(f"Requesting from: {target_url}")
        # Note: even a 20mb file takes a while to download from this site
        response: requests.Response = requests.get(url=target_url, verify=verify_ssl)
        print("Writing file")
        target_file.write(response.content)
        print("Done")

    return target_filename


def unzip_file(source_path: str, target_path: str) -> str:
    zipped_file: zipfile.ZipFile = zipfile.ZipFile(file=source_path, mode="r")
    zipped_file.extractall(path=target_path)

    csv_path: str = f"{target_path}/{zipped_file.namelist()[0]}"
    zipped_file.close()

    return csv_path


def gzip_file(source_path: str, target_path: str, year: int, month: int) -> str:
    gzip_path: str = f"{target_path}/{year}_{month}.csv.gz"

    if not os.path.exists(target_path):
        os.mkdir(target_path)

    # https://docs.python.org/3/library/gzip.html#examples-of-usage
    with open(source_path, "rb") as source_file:
        with gzip.open(gzip_path, "wb") as target_file:
            shutil.copyfileobj(source_file, target_file)

    return gzip_path


def load_to_gcs(source_file: str, bucket_name: str, target_folder: str) -> str:
    base_file_name: str = os.path.basename(source_file)
    target_file_path: str = f"{target_folder}/{base_file_name}"
    client: storage.Client = storage.Client()
    bucket: storage.Bucket = client.get_bucket(bucket_or_name=bucket_name)
    blob: storage.Blob = storage.Blob(name=target_file_path, bucket=bucket)
    blob.upload_from_filename(filename=source_file)

    return f"gs://{bucket_name}/{target_file_path}"


def load_to_bigquery(gcs_path: str, table_name: str) -> bigquery.Table:
    client: bigquery.Client = bigquery.Client()
    dataset: bigquery.Dataset = client.create_dataset(dataset="dsongcp", exists_ok=True)
    table_name_full: str = f"ajp-ds-gcp.{dataset.dataset_id}.{table_name}"

    job_config: bigquery.LoadJobConfig = bigquery.LoadJobConfig(
        schema=bq_schema.get_flights_raw_schema(),
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="FlightDate",
        ),
        write_disposition="WRITE_APPEND",
    )

    load_job: bigquery.LoadJob = client.load_table_from_uri(
        source_uris=gcs_path, destination=table_name_full, job_config=job_config
    )

    load_job.result()
    table: bigquery.Table = client.get_table(table=table_name_full)
    return table


def main() -> bigquery.Table:
    load_dotenv()
    target_bucket: str = "ajp-ds-gcp-flights"
    target_gcs_folder: str = "flights/raw"
    target_dir_dl: str = "../download/"
    target_dir_csv: str = "../csv"
    target_dir_gzip: str = "../gzip"
    year: int = 2015
    month: int = 1

    download_path: str = download_file(year=year, month=month, target_dir=target_dir_dl)
    csv_path: str = unzip_file(source_path=download_path, target_path=target_dir_csv)
    gzip_path: str = gzip_file(
        source_path=csv_path, target_path=target_dir_gzip, year=year, month=month
    )
    gcs_path: str = load_to_gcs(
        source_file=gzip_path,
        bucket_name=target_bucket,
        target_folder=target_gcs_folder,
    )

    table: bigquery.Table = load_to_bigquery(gcs_path=gcs_path, table_name="flights_raw")
    return table


if __name__ == "__main__":
    result: bigquery.Table = main()
    if result:
        print(f"Successfully uploaded file and loaded to BigQuery. Table is {result.table_id}")
