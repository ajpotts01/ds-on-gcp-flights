import csv
import apache_beam as beam
from apache_beam.pvalue import PValue


def main():
    with beam.Pipeline("DirectRunner") as pipeline:
        airports: PValue = (
            pipeline
            | beam.io.ReadFromText("airports.csv.gz")
            | beam.Map(lambda line: next(csv.reader([line])))
            # Airport code, then tuple/pair of lat/lon
            | beam.Map(lambda fields: (fields[0], (fields[21], fields[26])))
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
