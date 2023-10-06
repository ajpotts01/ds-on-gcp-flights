import os
import bigframes.pandas as bpd


def setup_project():
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


def map_columns(df_target: bpd.DataFrame, map: dict[str, str]) -> None:
    df_target = df_target[map.keys()].rename(columns=map)


def map_bools(
    df_target: bpd.DataFrame, columns: list[str], map: dict[str, bool]
) -> None:
    for col in columns:
        df_target[col] = df_target[col].map(map)


def map_floats(df_target: bpd.DataFrame, columns: list[str]) -> None:
    for col in columns:
        # Note: although it is tempting to use the static Dtypes
        # they actually don't work. Type conflicts.
        df_target[col] = df_target[col].astype("Float64")


def run_transforms(df_raw: bpd.DataFrame) -> bpd.DataFrame:
    df_transformed: bpd.DataFrame = df_raw.copy()

    # These functions will modify the dataframe in place
    # Normally would copy but no point for small ops and not sure how that goes with Bigframes
    map_columns(df_target=df_transformed, map=get_column_selection_map())
    map_bools(df_target=df_transformed, columns=get_columns_bool(), map=get_map_bool())
    map_floats(df_target=df_transformed, columns=get_columns_float())

    return df_transformed


def main():
    table_source: str = "dsongcp.flights_raw"
    table_target: str = "dsongcp.flights"

    df_flights_raw: bpd.DataFrame = bpd.read_gbq(query=table_source)
    df_flights_transformed: bpd.DataFrame = run_transforms(df_raw=df_flights_raw)
    df_flights_transformed.to_gbq(table_target, if_exists="replace")
