import argparse
import logging
import os
import shutil
import sys
import tempfile

from typing import Tuple

from dotenv import load_dotenv
from google.cloud import bigquery

from util import bq_schema, downloads, files, gcp, dates


def args(argv: list[str]) -> argparse.Namespace:
    args: argparse.Namespace = None

    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Ingest flight data from BTS website to GCS/Bigquery"
    )
    parser.add_argument("--bucket", help="Target GCS bucket", required=False)
    parser.add_argument(
        "--year", help="Year to search for. Example: 2015", required=False
    )
    parser.add_argument(
        "--month",
        help="Month to search for - without leading zeroes. e.g. 1, 2, 10, etc.",
        required=False,
    )

    try:
        args = parser.parse_args(args=argv)
    except Exception as ex:
        print(f"Error parsing arguments: {ex}")
        args = None
    finally:
        return args


def handle_defaults(year: str, month: str, bucket: str) -> Tuple[str, str, str]:
    target_bucket: str
    target_year: str
    target_month: str
    target_gcs_folder: str = "flights/raw"

    if bucket is None:
        target_bucket = os.getenv("BUCKET", None)

    if year is not None and month is not None:
        target_year = year
        target_month = month
    else:
        target_year, target_month = gcp.get_latest_month(
            bucket_name=target_bucket, target_folder=target_gcs_folder
        )
        target_year, target_month = dates.get_next_month(
            year=target_year, month=target_month
        )

    return target_bucket, target_year, target_month


def cli_main(args: argparse.Namespace) -> bigquery.Table:
    target_bucket: str
    year: str
    month: str
    target_gcs_folder: str = "flights/raw"

    target_bucket, year, month = handle_defaults(
        year=args.year, month=args.month, bucket=args.bucket
    )

    if target_bucket is not None and year is not None and month is not None:
        return main(
            target_bucket=target_bucket,
            target_gcs_folder=target_gcs_folder,
            year=year,
            month=month,
        )
    return None


def main(
    target_bucket: str, target_gcs_folder: str, year: str, month: str
) -> bigquery.Table:
    load_dotenv()
    table: bigquery.Table = None
    target_project: str = os.getenv("PROJECT", None)
    target_dataset: str = os.getenv("DATASET", None)

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
    cli_args: argparse.Namespace = args(sys.argv[1:])

    if cli_args:
        table: bigquery.Table = cli_main(args=cli_args)
        if table:
            print(
                f"Successfully uploaded file and loaded to BigQuery. Table is {table.table_id}"
            )
        else:
            print(f"Table not successfully updated.")
    else:
        print("Command line arguments are required.")
