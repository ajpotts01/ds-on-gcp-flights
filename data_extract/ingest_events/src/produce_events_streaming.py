import argparse
import datetime as dt
import logging
import time

import pytz

from google.cloud import bigquery, pubsub

TIME_FORMAT = "%Y-%m-%d %H:%M:%S %Z"
RFC3339_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S-00:00"


def get_event_query() -> str:
    query: str = """
    select
      event_type,
      event_time as notify_time,
      event_data
    from
      dsongcp.flights_sim_events
    where
      event_time >= @start_time
      and event_time < @end_time
    order by
      event_time asc
"""
    return query


def get_time_to_sleep(
    notify_time: dt.datetime,
    script_start_time: dt.datetime,
    sim_start_time: dt.datetime,
    speed_factor: int,
) -> int:
    time_elapsed: int = (dt.datetime.utcnow() - script_start_time).seconds

    sim_time_elapsed: int = (notify_time - sim_start_time).seconds / speed_factor

    time_to_sleep: int = sim_time_elapsed - time_elapsed
    return time_to_sleep


def setup_topics(
    project: str, event_types: list, pubsub_publisher: pubsub.PublisherClient
) -> dict:
    # The book uses this time to create topics
    # This project controls them with Terraform, so no need
    return {
        event_type: pubsub_publisher.topic_path(
            project=project, topic=event_type.replace("_", "-")
        )
        for event_type in event_types
    }


def publish_events(
    pubsub_publisher: pubsub.PublisherClient,
    topics: dict,
    notifications: dict,
    event_time: dt.datetime,
):
    timestamp: str = event_time.strftime(RFC3339_TIME_FORMAT)
    for key, topic in topics.items():
        events: list = notifications[key]
        next_event: str
        for next_event in events:
            pubsub_publisher.publish(
                topic, next_event.encode(), EventTimeStamp=timestamp
            )


def send_notifications(
    publisher: pubsub.PublisherClient,
    topics: dict,
    data: bigquery.QueryJob,
    sim_start_time: dt.datetime,
    script_start_time: dt.datetime,
    speed_factor: int,
) -> bool:
    # Separate list to track items sent to each topic
    notifications: dict[str, list] = {x: [] for x in topics.keys()}

    for next_row in data:
        event, event_time, event_data = next_row

        if (
            get_time_to_sleep(
                notify_time=event_time,
                script_start_time=script_start_time,
                sim_start_time=sim_start_time,
                speed_factor=speed_factor,
            )
            > 1
        ):
            publish_events(
                pubsub_publisher=publisher,
                topics=topics,
                notifications=notifications,
                event_time=event_time,
            )
            # Reset buffer
            notifications = {x: [] for x in topics.keys()}

            new_sleep_time: int = get_time_to_sleep(
                notify_time=event_time,
                script_start_time=script_start_time,
                sim_start_time=sim_start_time,
                speed_factor=speed_factor,
            )

            if new_sleep_time > 0:
                logging.info(f"Sleeping {new_sleep_time} seconds")
                time.sleep(new_sleep_time)

        notifications[event].append(event_data)

    # Clear buffer if any left over
    publish_events(
        pubsub_publisher=publisher,
        topics=topics,
        notifications=notifications,
        event_time=event_time,
    )

    return True


def main(args: argparse.Namespace):
    script_start_time: dt.datetime = dt.datetime.utcnow()
    sim_start_time: dt.datetime = dt.datetime.strptime(
        args.start_time, TIME_FORMAT
    ).replace(tzinfo=pytz.UTC)
    query: str = get_event_query()

    bq_client: bigquery.Client = bigquery.Client()
    publisher: pubsub.PublisherClient = pubsub.PublisherClient()
    bq_job: bigquery.QueryJobConfig = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", args.start_time),
            bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", args.end_time),
        ]
    )

    result: bigquery.QueryJob = bq_client.query(query=query, job_config=bq_job)
    event_types: list = ["wheels_off", "arrived", "departed"]
    topics: dict = setup_topics(
        project=args.project, event_types=event_types, pubsub_publisher=publisher
    )

    success: bool = send_notifications(
        publisher=publisher,
        topics=topics,
        data=result,
        sim_start_time=sim_start_time,
        script_start_time=script_start_time,
        speed_factor=args.speed_factor,
    )

    return success


def parse_args() -> argparse.Namespace:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Send simulated flight events to Cloud Pub/Sub"
    )
    parser.add_argument(
        "--start_time", help="Example: 2015-05-01 00:00:00 UTC", required=True
    )
    parser.add_argument(
        "--end_time", help="Example: 2015-05-03 00:00:00 UTC", required=True
    )
    parser.add_argument(
        "--project", help="your project id, to create pubsub topic", required=True
    )
    parser.add_argument(
        "--speed_factor",
        help="Example: 60 implies 1 hour of data sent to Cloud Pub/Sub in 1 minute",
        required=True,
        type=float,
    )
    parser.add_argument(
        "--jitter",
        help="type of jitter to add: None, uniform, exp  are the three options",
        default="None",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args: argparse.Namespace = parse_args()
    main(args=args)
