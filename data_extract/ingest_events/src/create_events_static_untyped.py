import csv
import datetime as dt
import json
import logging
from typing import Generator, Tuple

import apache_beam as beam
import pytz

from apache_beam.pvalue import PValue
from apache_beam.transforms.combiners import Sample
from pytz.tzinfo import StaticTzInfo

DT_FORMAT = "%Y-%m-%d %H:%M:%S"


def create_event_row(fields):
    row = dict(fields)
    row["event_data"] = json.dumps(fields)
    return row


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


def get_next_event(fields):
    print("get_next_event")
    json_fields = fields  # json.loads(fields)

    if len(json_fields["dep_time"]) > 0:
        event = dict(json_fields)  # copy
        event["event_type"] = "departed"
        event["event_time"] = json_fields["dep_time"]
        for field in [
            "taxi_out",
            "wheels_off",
            "wheels_on",
            "taxi_in",
            "arr_time",
            "arr_delay",
            "distance",
        ]:
            event.pop(field, None)  # Remove fields - not knowable on departure
        yield event

    if len(json_fields["arr_time"]) > 0:
        event = dict(json_fields)
        event["event_type"] = "arrived"
        event["event_time"] = json_fields["arr_time"]
        yield event

    # This is in the book but not in the sample code?
    if len(json_fields["wheels_off"]) > 0:
        event = dict(json_fields)
        event["event_type"] = "wheels_off"
        event["event_time"] = json_fields["wheels_off"]
        for field in ["wheels_on", "taxi_in", "arr_time", "arr_delay", "distance"]:
            event.pop(field, None)
        yield event


def correct_arrival_time(arrival_time, departure_time):
    print("correct_arrival_time")
    if (
        len(arrival_time) > 0
        and len(departure_time) > 0
        and arrival_time < departure_time
    ):
        arrival_dt = dt.datetime.strptime(arrival_time, DT_FORMAT)
        arrival_dt += dt.timedelta(hours=24)
        return arrival_dt.strftime(DT_FORMAT)
    else:
        return arrival_time


def convert_to_utc(date, time, time_zone):
    print("convert_to_utc")
    try:
        if len(time) > 0 and time_zone is not None:
            local_timezone = pytz.timezone(time_zone)
            local_datetime = local_timezone.localize(
                # dt.datetime.combine(date, dt.datetime.min.time()), is_dst=False
                dt.datetime.strptime(date, "%Y-%m-%d"),
                is_dst=False,
            )
            print(f"{type(local_datetime)=}")
            print(f"{local_datetime=}")

            local_datetime += dt.timedelta(hours=int(time[:2]), minutes=int(time[2:]))
            utc_datetime = local_datetime.astimezone(pytz.utc)
            return (
                utc_datetime.strftime(DT_FORMAT),
                local_datetime.utcoffset().total_seconds(),
            )
        else:
            print("Skipped conversion")
            print(f"{time=}")
            print(f"{time_zone=}")
            return "", 0
    except (ValueError, AttributeError) as ex:
        logging.error(f"Error encountered with values: {date=}, {time=}, {time_zone=}")
        raise ex


# Confirm data type...
def correct_time_zone(fields, airport_timezones):
    print("correct_time_zone")
    try:
        airport_origin = fields["origin_airport_seq_id"]
        airport_destination = fields["dest_airport_seq_id"]

        timezone_origin = airport_timezones[airport_origin][2]  # ???
        timezone_destination = airport_timezones[airport_destination][2]  # ???

        for field in ["crs_dep_time", "dep_time", "wheels_off"]:
            fields[field], departure_timezone = convert_to_utc(
                fields["flight_date"], fields[field], timezone_origin
            )
        for field in ["wheels_on", "crs_arr_time", "arr_time"]:
            fields[field], arrival_timezone = convert_to_utc(
                fields["flight_date"], fields[field], timezone_destination
            )

        for field in ["wheels_on", "wheels_off", "crs_arr_time", "arr_time"]:
            fields[field] = correct_arrival_time(fields[field], fields["dep_time"])

        print("Setting timezone fields")
        fields["dep_airport_lat"] = airport_timezones[airport_origin][0]
        fields["dep_airport_lon"] = airport_timezones[airport_origin][1]
        fields["arr_airport_lat"] = airport_timezones[airport_destination][0]
        fields["arr_airport_lon"] = airport_timezones[airport_destination][1]
        fields["dep_airport_tzoffset"] = departure_timezone
        fields["arr_airport_tzoffset"] = arrival_timezone
        # Is this necessary?
        # fields["flight_date"] = dt.datetime.strftime(fields["flight_date"], "%Y-%m-%d")
        print("Yielding")
        yield fields
    except KeyError as ex:
        logging.exception("Unknown airport - skipping row")


def add_time_zone(lat, lon):
    result = ()
    print("add_time_zone")
    try:
        import timezonefinder  # No top-level imports? Must be due to parallelization

        tz_finder = timezonefinder.TimezoneFinder()
        tz = tz_finder.timezone_at(lng=float(lon), lat=float(lat))

        if tz is None:
            tz = "UTC"

        # Return the original lat/lon for joining strings later
        result = (lat, lon, tz)
    except ValueError as ex:
        # Header!
        result = ("", "", "")

        if len(lat) > 0 and len(lon) > 0:
            result = (lat, lon, "TIMEZONE")
    finally:
        return result


def main():
    # This code is a mess.
    # Since the book was written, Beam's probably been upgraded to be better about types.
    # This is great, but the downside is that a lot of the code made assumptions about
    # whether something was a string or a datetime which are no longer true with this
    # version of Beam.
    # The code in the public repo doesn't help much either. I suspect if one was to clone
    # and immediately run it, it wouldn't work out of the box.
    project = "ajp-ds-gcp"
    bucket = "ajp-ds-gcp-flights"
    region = "australia-southeast1"

    pipeline_args = [
        f"--project={project}",
        "--job_name=flight-events-batch",
        "--save_main_session",
        f"--staging_location=gs://{bucket}/flights/staging/",
        f"--temp_location=gs://{bucket}/flights/temp/",
        "--setup_file=./setup.py",
        "--max_num_workers=8",
        f"--region={region}",
        "--runner=DataflowRunner",
        "--service_account_email=ds-gcp-ingestion@ajp-ds-gcp.iam.gserviceaccount.com",
    ]

    with beam.Pipeline(argv=pipeline_args) as pipeline:
        path_airports = f"gs://{bucket}/flights/airports/airports.csv.gz"
        path_flights = f"gs://{bucket}/flights/tz_corrections/all_flights"

        print("Airports...")
        airports = (
            pipeline
            | "airports: read" >> beam.io.ReadFromText(path_airports)
            # The book just has the USA filter, but that means the header gets filtered out...
            | "airports: filter usa"
            >> beam.Filter(lambda line: "United States" in line)
            | "airports: fields" >> beam.Map(lambda line: next(csv.reader([line])))
            | "airports: filter empty"
            >> beam.Filter(lambda fields: len(fields[21]) > 0)
            # Airport code, then tuple/pair of lat/lon
            | "airports: timezones"
            >> beam.Map(
                lambda fields: (fields[0], add_time_zone(fields[21], fields[26]))
            )
        )

        print("Flights...")
        flights = (
            pipeline
            | "flights: read"
            >> beam.io.ReadFromBigQuery(
                query="SELECT * FROM dsongcp.flights_v",
                use_standard_sql=True,
                # gcs_location=f"gs://{bucket}/flights/temp",
                # project="ajp-ds-gcp",
            )
            | "flights: tz correction"
            >> beam.FlatMap(correct_time_zone, beam.pvalue.AsDict(airports))
        )

        flights_schema = get_flights_schema()

        print("Writing flights...")
        (
            flights
            # Trying removing json.dumps here
            # The result indicates it's already JSON string by this point
            # | "flights: json" >> beam.Map(lambda fields: json.dumps(fields))
            | "flights: write to bq"
            >> beam.io.WriteToBigQuery(
                f"{project}:dsongcp.flights_tz_corrected",
                schema=flights_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                # custom_gcs_temp_location=f"gs://{bucket}/flights/temp",
                # project="ajp-ds-gcp",
            )
            # | "flights: write" >> beam.io.WriteToText(path_flights)
        )

        events_schema = get_events_schema()

        print("Events...")
        events: PValue = flights | beam.FlatMap(get_next_event)
        (
            events
            | "events: to_row" >> beam.Map(lambda fields: create_event_row(fields))
            | "events: write"
            >> beam.io.WriteToBigQuery(
                f"{project}:dsongcp.flights_sim_events",
                schema=events_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    main()
