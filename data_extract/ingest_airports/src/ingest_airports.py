import csv
import apache_beam as beam
from apache_beam.pvalue import PValue


def main():
    with beam.Pipeline("DirectRunner") as pipeline:
        airports: PValue = (
            pipeline
            | beam.io.ReadFromText("airports.csv.gz")
            | beam.Map(lambda line: next(csv.reader(line)))
            | beam.Map(lambda fields: (fields[0], fields[21], fields[26]))
        )


if __name__ == "__main__":
    main()
