import os
import shutil
import tempfile

from dotenv import load_dotenv
from google.cloud import bigquery

from util import bq_schema, downloads, files, gcp


def main() -> bigquery.Table:
    load_dotenv()
    table: bigquery.Table = None
    target_bucket: str = os.getenv("BUCKET", None)
    target_project: str = os.getenv("PROJECT", None)
    target_dataset: str = os.getenv("DATASET", None)
    target_gcs_folder: str = "flights/raw"
    year: int = 2015
    month: int = 1

    try:
        temp_dir: str = tempfile.mkdtemp(prefix="ingest_flights")

        download_path: str = downloads.download_file(
            year=year, month=month, target_dir=temp_dir
        )
        csv_path: str = files.unzip_file(
            source_path=download_path, target_path=temp_dir
        )
        gzip_path: str = files.gzip_file(
            source_path=csv_path, target_path=temp_dir, year=year, month=month
        )
        gcs_path: str = gcp.load_to_gcs(
            source_file=gzip_path,
            bucket_name=target_bucket,
            target_folder=target_gcs_folder,
        )

        table = gcp.load_to_bigquery(
            gcs_path=gcs_path,
            table_name="flights_raw",
            dataset=target_dataset,
            project=target_project,
        )
    except Exception as ex:
        print(f"Error retrieving data: {ex}")
        table = None
    finally:
        shutil.rmtree(temp_dir)
        return table


if __name__ == "__main__":
    table: bigquery.Table = main()
    if table:
        print(
            f"Successfully uploaded file and loaded to BigQuery. Table is {table.table_id}"
        )
