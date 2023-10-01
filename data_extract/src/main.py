import logging

from extract_flights import main, handle_defaults

from flask import escape, Flask, Request
from google.cloud import bigquery

app: Flask = Flask(__name__)


@app.route("/", methods=["POST"])
def extract_flights(request: Request) -> bigquery.Table:
    try:
        target_bucket: str
        target_year: str
        target_month: str

        request_json: dict = request.get_json()

        year: str = escape(s=request_json["year"]) if "year" in request_json else None
        month: str = (
            escape(s=request_json["month"]) if "month" in request_json else None
        )
        bucket: str = (
            escape(s=request_json["bucket"]) if "year" in request_json else None
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
