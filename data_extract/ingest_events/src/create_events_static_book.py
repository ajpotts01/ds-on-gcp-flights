#!/usr/bin/env python3

# Copyright 2016 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import apache_beam as beam
import logging
import csv
import json

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def addtimezone(lat, lon):
    print("add_time_zone")
    try:
        import timezonefinder

        tf = timezonefinder.TimezoneFinder()
        lat = float(lat)
        lon = float(lon)
        return lat, lon, tf.timezone_at(lng=lon, lat=lat)
    except ValueError:
        return lat, lon, "TIMEZONE"  # header


def as_utc(date, hhmm, tzone):
    print("convert_to_utc")
    """
    Returns date corrected for timezone, and the tzoffset
    """
    try:
        if len(hhmm) > 0 and tzone is not None:
            import datetime, pytz

            loc_tz = pytz.timezone(tzone)
            loc_dt = loc_tz.localize(
                datetime.datetime.strptime(date, "%Y-%m-%d"), is_dst=False
            )
            # can't just parse hhmm because the data contains 2400 and the like ...
            loc_dt += datetime.timedelta(hours=int(hhmm[:2]), minutes=int(hhmm[2:]))
            utc_dt = loc_dt.astimezone(pytz.utc)
            return utc_dt.strftime(DATETIME_FORMAT), loc_dt.utcoffset().total_seconds()
        else:
            return "", 0  # empty string corresponds to canceled flights
    except ValueError as e:
        logging.exception("{} {} {}".format(date, hhmm, tzone))
        raise e


def add_24h_if_before(arrtime, deptime):
    print("correct_arrival_time")
    import datetime

    if len(arrtime) > 0 and len(deptime) > 0 and arrtime < deptime:
        adt = datetime.datetime.strptime(arrtime, DATETIME_FORMAT)
        adt += datetime.timedelta(hours=24)
        return adt.strftime(DATETIME_FORMAT)
    else:
        return arrtime


def tz_correct(fields, airport_timezones):
    print("Correcting time zone")
    try:
        # convert all times to UTC
        dep_airport_id = fields["origin_airport_seq_id"]
        arr_airport_id = fields["dest_airport_seq_id"]
        dep_timezone = airport_timezones[dep_airport_id][2]
        arr_timezone = airport_timezones[arr_airport_id][2]

        for f in ["crs_dep_time", "dep_time", "wheels_off"]:
            fields[f], deptz = as_utc(fields["flight_date"], fields[f], dep_timezone)
        for f in ["wheels_on", "crs_arr_time", "arr_time"]:
            fields[f], arrtz = as_utc(fields["flight_date"], fields[f], arr_timezone)

        for f in ["wheels_off", "wheels_on", "crs_arr_time", "arr_time"]:
            fields[f] = add_24h_if_before(fields[f], fields["dep_time"])

        fields["dep_airport_lat"] = airport_timezones[dep_airport_id][0]
        fields["dep_airport_lon"] = airport_timezones[dep_airport_id][1]
        fields["dep_airport_tzoffset"] = deptz
        fields["arr_airport_lat"] = airport_timezones[arr_airport_id][0]
        fields["arr_airport_lon"] = airport_timezones[arr_airport_id][1]
        fields["arr_airport_tzoffset"] = arrtz

        print("Yielding")
        yield fields
    except KeyError as e:
        logging.exception(f"Ignoring {fields} because airport is not known")


def get_next_event(fields):
    print("get_next_event")
    if len(fields["dep_time"]) > 0:
        event = dict(fields)  # copy
        event["event_type"] = "departed"
        event["event_time"] = fields["dep_time"]
        for f in [
            "taxi_out",
            "wheels_off",
            "wheels_on",
            "taxi_in",
            "arr_time",
            "arr_delay",
            "distance",
        ]:
            event.pop(f, None)  # not knowable at departure time
        yield event
    if len(fields["arr_time"]) > 0:
        event = dict(fields)
        event["event_type"] = "arrived"
        event["event_time"] = fields["arr_time"]
        yield event


def run():
    with beam.Pipeline("DirectRunner") as pipeline:
        airports = (
            pipeline
            | "airports:read" >> beam.io.ReadFromText("airports.csv.gz")
            | beam.Filter(lambda line: "United States" in line)
            | "airports:fields" >> beam.Map(lambda line: next(csv.reader([line])))
            | "airports:tz"
            >> beam.Map(lambda fields: (fields[0], addtimezone(fields[21], fields[26])))
        )

        flights = (
            pipeline
            | "flights:read"
            >> beam.io.ReadFromBigQuery(
                query="SELECT * FROM dsongcp.flights_v WHERE rand() < 0.001",
                use_standard_sql=True,
                gcs_location=f"gs://ajp-ds-gcp-flights/flights/temp",
                project="ajp-ds-gcp",
            )
            | "flights:tzcorr" >> beam.FlatMap(tz_correct, beam.pvalue.AsDict(airports))
        )

        (
            flights
            | "flights:tostring" >> beam.Map(lambda fields: json.dumps(fields))
            | "flights:out" >> beam.io.textio.WriteToText("all_flights")
        )

        events = flights | beam.FlatMap(get_next_event)

        (
            events
            | "events:tostring" >> beam.Map(lambda fields: json.dumps(fields))
            | "events:out" >> beam.io.textio.WriteToText("all_events")
        )


if __name__ == "__main__":
    run()
