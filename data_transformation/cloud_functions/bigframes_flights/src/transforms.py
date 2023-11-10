import os
import bigframes.pandas as bpd
from google.cloud import bigquery
from dotenv import load_dotenv


def setup_project():
    load_dotenv()
    bpd.options.bigquery.project = os.getenv("PROJECT")
    bpd.options.bigquery.location = os.getenv("LOCATION")


def get_column_selection_map() -> dict[str, str]:
    return {
        "FlightDate": "flight_date",
        "Reporting_Airline": "unique_carrier",
        "OriginAirportSeqID": "origin_airport_seq_id",
        "Origin": "origin",
        "DestAirportSeqID": "dest_airport_seq_id",
        "Dest": "dest",
        "CRSDepTime": "crs_dep_time",
        "DepTime": "dep_time",
        "DepDelay": "dep_delay",
        "TaxiOut": "taxi_out",
        "WheelsOff": "wheels_off",
        "WheelsOn": "wheels_on",
        "TaxiIn": "taxi_in",
        "CRSArrTime": "crs_arr_time",
        "ArrTime": "arr_time",
        "ArrDelay": "arr_delay",
        "Cancelled": "cancelled",
        "Diverted": "diverted",
        "Distance": "distance",
    }


def get_columns_float() -> list[str]:
    return [
        "dep_delay",
        "taxi_out",
        "taxi_in",
        "arr_delay",
    ]


def get_map_bool() -> dict[str, bool]:
    return {
        "1.00": True,
        "0.00": False,
    }


def get_columns_bool() -> list[str]:
    return ["cancelled", "diverted"]


def map_columns(df_target: bpd.DataFrame, map: dict[str, str]) -> bpd.DataFrame:
    df_target = df_target[map.keys()].rename(columns=map)
    return df_target


def map_bools(
    df_target: bpd.DataFrame, columns: list[str], map: dict[str, bool]
) -> bpd.DataFrame:
    for col in columns:
        df_target[col] = df_target[col].map(map)

    return df_target


def map_floats(df_target: bpd.DataFrame, columns: list[str]) -> bpd.DataFrame:
    for col in columns:
        # Note: although it is tempting to use the static Dtypes
        # they actually don't work. Type conflicts.
        df_target[col] = df_target[col].astype("Float64")

    return df_target


def run_transforms(df_raw: bpd.DataFrame) -> bpd.DataFrame:
    df_transformed: bpd.DataFrame = df_raw.copy()

    # These functions will modify the dataframe in place
    # Normally would copy but no point for small ops and not sure how that goes with Bigframes
    df_transformed = map_columns(
        df_target=df_transformed, map=get_column_selection_map()
    )
    df_transformed = map_bools(
        df_target=df_transformed, columns=get_columns_bool(), map=get_map_bool()
    )
    df_transformed = map_floats(df_target=df_transformed, columns=get_columns_float())

    

    return df_transformed


def create_new_table(cols: dict[str, str], table_name: str):
    bq_client: bigquery.Client = bigquery.Client()
    project: str = os.getenv("PROJECT")

    full_table_name: str = f"{project}.{table_name}"

    schema: list[bigquery.SchemaField] = []
    for new_col_name in cols.values():
        col_type: str = "STRING"
        if new_col_name in get_columns_bool():
            col_type = "BOOLEAN"
        if new_col_name in get_columns_float():
            col_type = "FLOAT"
        if new_col_name == "flight_date":
            col_type = "DATE"

        schema.append(bigquery.SchemaField(new_col_name, col_type, mode="NULLABLE"))

    # Weird workaround. Can't do truncate table...
    table: bigquery.Table = bigquery.Table(full_table_name, schema=schema)
    
    bq_client.delete_table(table=table, not_found_ok=True)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="flight_date"
    )

    table = bq_client.create_table(table, exists_ok=True)

    return table


def main():
    setup_project()

    table_source: str = "dsongcp.flights_raw"
    table_target: str = "dsongcp.flights"

    # Note: Bigframes does not appear to support partitioning tables at this time.
    # Try to create the table using the client API, leaving it alone if it already exists.
    table: bigquery.Table = create_new_table(
        cols=get_column_selection_map(), table_name=table_target
    )

    bpd.pandas.set_option("display.max_colwidth", None)

    df_flights_raw: bpd.DataFrame = bpd.read_gbq(query_or_table=table_source)
    df_flights_transformed: bpd.DataFrame = run_transforms(df_raw=df_flights_raw)

    if table:
        df_flights_transformed.to_gbq(table_target, if_exists="append")
