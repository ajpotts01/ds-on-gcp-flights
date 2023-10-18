import csv
from typing import Tuple

import apache_beam as beam
from apache_beam.pvalue import PValue
from apache_beam.transforms.combiners import Sample


def test_time_zone(lat: str, lon: str) -> str:
    import timezonefinder

    tz: timezonefinder.TimezoneFinder = timezonefinder.TimezoneFinder()
    result = tz.timezone_at(lng=lon, lat=lat)
    print(f"{result=}")


def add_time_zone(lat: str, lon: str) -> Tuple[float, float, str]:
    result: Tuple[float, float, str] = ()

    try:
        import timezonefinder  # No top-level imports? Must be due to parallelization

        tz_finder: timezonefinder.TimezoneFinder = timezonefinder.TimezoneFinder()
        tz: str = tz_finder.timezone_at(lng=float(lon), lat=float(lat))

        if tz is None:
            tz = "UTC"

        # Return the original lat/lon for joining strings later
        result = (lat, lon, tz)
    except ValueError as ex:
        # Header!
        print("Found header")
        result = ("", "", "")

        if len(lat) > 0 and len(lon) > 0:
            result = (lat, lon, "TIMEZONE")
    finally:
        print(result)
        return result


def main():
    with beam.Pipeline("DirectRunner") as pipeline:
        airports: PValue = (
            pipeline
            | "airports: read" >> beam.io.ReadFromText("airports.csv.gz")
            # The book just has the USA filter, but that means the header gets filtered out...
            | "airports: filter usa"
            >> beam.Filter(
                lambda line: "United States" in line or "AIRPORT_SEQ_ID" in line
            )
            | "airports: fields" >> beam.Map(lambda line: next(csv.reader([line])))
            | "airports: filter empty" >> beam.Filter(lambda fields: len(fields[21]) > 0)
            # Airport code, then tuple/pair of lat/lon
            | "airports: timezones"
            >> beam.Map(
                lambda fields: (fields[0], add_time_zone(fields[21], fields[26]))
            )
        )

        (
            airports
            | beam.Map(
                lambda airport_data: f"{airport_data[0]},{','.join(airport_data[1])}"
            )
            | beam.io.WriteToText("extracted_airports")
        )


if __name__ == "__main__":
    main()
