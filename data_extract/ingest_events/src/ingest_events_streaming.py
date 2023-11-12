import json

import apache_beam as beam
import numpy as np

from apache_beam.transforms import window


def get_stats_schema():
    return ",".join(
        [
            "airport:STRING",
            "avg_arr_delay:FLOAT",
            "avg_dep_delay:FLOAT",
            "num_flights:INT64",
            "start_time:TIMESTAMP",
            "end_time:TIMESTAMP",
        ]
    )


def get_events_schema():
    return ",".join(
        [
            get_flights_schema(),
            "event_type:STRING",
            "event_time:TIMESTAMP",
            "event_data:STRING",
        ]
    )


def get_flights_schema():
    return ",".join(
        [
            "flight_date:DATE",
            "unique_carrier:STRING",
            "origin_airport_seq_id:STRING",
            "origin:STRING",
            "dest_airport_seq_id:STRING",
            "dest:STRING",
            "crs_dep_time:TIMESTAMP",
            "dep_time:TIMESTAMP",
            "dep_delay:FLOAT",
            "taxi_out:FLOAT",
            "wheels_off:TIMESTAMP",
            "wheels_on:TIMESTAMP",
            "taxi_in:FLOAT",
            "crs_arr_time:TIMESTAMP",
            "arr_time:TIMESTAMP",
            "arr_delay:FLOAT",
            "cancelled:BOOLEAN",
            "diverted:BOOLEAN",
            "distance:FLOAT",
            "dep_airport_lat:FLOAT",
            "dep_airport_lon:FLOAT",
            "dep_airport_tzoffset:FLOAT",
            "arr_airport_lat:FLOAT",
            "arr_airport_lon:FLOAT",
            "arr_airport_tzoffset:FLOAT",
            "year:STRING",
        ]
    )


def by_airport(event):
    if event["event_type"] == "departed":
        return event["origin"], event
    return event["dest"], event


def compute_stats(airport, events):
    arrived = [
        event["arr_delay"] for event in events if event["event_type"] == "arrived"
    ]
    departed = [
        event["dep_delay"] for event in events if event["event_type"] == "departed"
    ]
    avg_arr_delay = float(np.mean(arrived)) if len(arrived) > 0 else None
    avg_dep_delay = float(np.mean(departed)) if len(departed) > 0 else None

    num_flights = len(events)
    start_time = min([event["event_time"] for event in events])
    latest_time = max([event["event_time"] for event in events])

    return {
        "airport": airport,
        "avg_arr_delay": avg_arr_delay,
        "avg_dep_delay": avg_dep_delay,
        "num_flights": num_flights,
        "start_time": start_time,
        "end_time": latest_time,
    }


def main():
    project = "ajp-ds-gcp"
    bucket = "ajp-ds-gcp-flights"
    region = "australia-southeast1"

    pipeline_args = [
        f"--project={project}",
        "--job_name=flight-events",
        "--save_main_session",
        f"--staging_location=gs://{bucket}/flights/staging/",
        f"--temp_location=gs://{bucket}/flights/temp/",
        "--setup_file=./setup.py",
        "--max_num_workers=8",
        f"--region={region}",
        "--runner=DataflowRunner",
        "--service_account_email=ds-gcp-ingestion@ajp-ds-gcp.iam.gserviceaccount.com",
        "--streaming",
    ]

    with beam.Pipeline(argv=pipeline_args) as pipeline:
        topics = ["arrived", "departed", "wheels-off"]
        topic_pipelines = {}

        for topic in topics:
            topic_path = f"projects/{project}/topics/{topic}"
            events = (
                pipeline
                | f"{topic}: read"
                >> beam.io.ReadFromPubSub(
                    topic=topic_path, timestamp_attribute="EventTimeStamp"
                )
                | f"{topic}: parse" >> beam.Map(lambda data: json.loads(data))
            )
            topic_pipelines[topic] = events

        all_events = (
            topic_pipelines["arrived"],
            topic_pipelines["departed"],
        ) | beam.Flatten()

        schema = get_events_schema()
        (
            all_events
            | "all: bigquery"
            >> beam.io.WriteToBigQuery(
                "dsongcp.streaming_events",
                schema=schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

        stats_schema = get_stats_schema()

        stats = (
            all_events
            | "map: airport" >> beam.Map(by_airport)
            | "window" >> beam.WindowInto(window.SlidingWindows(60 * 60, 5 * 60))
            | "group: key" >> beam.GroupByKey()
            | "stats" >> beam.Map(lambda row: compute_stats(row[0], row[1]))
        )

        (
            stats
            | "write: bq"
            >> beam.io.WriteToBigQuery(
                "dsongcp.streaming_delays",
                schema=stats_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    main()
