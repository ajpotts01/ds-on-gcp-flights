import os
from typing import Tuple

from google.cloud import storage, bigquery
from util import bq_schema


def load_to_gcs(source_file: str, bucket_name: str, target_folder: str) -> str:
    base_file_name: str = os.path.basename(source_file)
    target_file_path: str = f"{target_folder}/{base_file_name}"
    client: storage.Client = storage.Client()
    bucket: storage.Bucket = client.get_bucket(bucket_or_name=bucket_name)
    blob: storage.Blob = storage.Blob(name=target_file_path, bucket=bucket)
    blob.upload_from_filename(filename=source_file)

    return f"gs://{bucket_name}/{target_file_path}"


def load_to_bigquery(
    gcs_path: str, table_name: str, dataset: str, project: str
) -> bigquery.Table:
    client: bigquery.Client = bigquery.Client()
    dataset: bigquery.Dataset = client.create_dataset(dataset=dataset, exists_ok=True)
    table_name_full: str = f"{project}.{dataset.dataset_id}.{table_name}"

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


def get_latest_month(bucket_name: str, target_folder: str) -> Tuple[str, str]:
    client: storage.Client = storage.Client()

    bucket: storage.Bucket = client.get_bucket(bucket_or_name=bucket_name)
    blobs: list[storage.Blob] = list(bucket.list_blobs(prefix=target_folder))

    if len(blobs) > 0:
        files: list[str] = [x.name for x in blobs if "csv" in x.name]
        last_file: str = os.path.basename(files[-1])
        year_month: str = last_file.split("_")
        year: str = year_month[0]
        month: str = year_month[1].replace(".csv.gz", "")

        return year, month

    return None, None
