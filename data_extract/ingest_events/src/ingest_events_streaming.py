from google.cloud import bigquery


def get_event_query() -> str:
    query: str = """
    select
      event_type,
      event_time as notify_time,
      event_data
    from
      dsongcp.flights_simevents
    where
      event_time >= @start_time
      and event_time < @end_time
    order by
      event_time asc
"""
    return query


def main(args):
    query: str = get_event_query()

    bq_client: bigquery.Client = bigquery.Client()
    bq_job: bigquery.QueryJobConfig = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", args.start_time),
            bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", args.end_time),
        ]
    )

    result: bigquery.QueryJob = bq_client.query(query=query, job_config=bq_job)


main()
