import logging
import os
from extract_flights import main, handle_defaults

import google.cloud.logging
from flask import escape, Flask, request
from google.cloud import bigquery

app: Flask = Flask(__name__)


def setup_gcp_logging():
    if os.getenv("ENV", "DEV") == "PROD":
        client: google.cloud.logging.Client = google.cloud.logging.Client()
        client.setup_logging() # This will hook in with all stdlib logging calls from here on

# Note this differs from the book:
# Adding "request" as a param in the function signature does not work anymore.
@app.route("/", methods=["POST"])
def extract_flights() -> str:
    try:
        target_bucket: str
        target_year: str
        target_month: str

        request_json: dict = request.get_json()
        setup_gcp_logging()
        logging.info("Logging started")
        logging.info(request)
        logging.info(f"{request_json=}")

        year: str = escape(request_json["year"]) if "year" in request_json else None
        month: str = escape(request_json["month"]) if "month" in request_json else None
        bucket: str = (
            escape(request_json["bucket"]) if "bucket" in request_json else None
        )
        target_gcs_folder: str = "flights/raw"
        table: bigquery.Table = None

        target_bucket, target_year, target_month = handle_defaults(
            year=year, month=month, bucket=bucket
        )
        if (
            target_bucket is not None
            and target_year is not None
            and target_month is not None
        ):
            table = main(
                target_bucket=target_bucket,
                target_gcs_folder=target_gcs_folder,
                year=target_year,
                month=target_month,
            )

        if table:
            return f"Success: table ID is {table.table_id}"
        else:
            return f"Failed to load to BigQuery."
    except Exception as ex:
        logging.exception(f"{ex}")
