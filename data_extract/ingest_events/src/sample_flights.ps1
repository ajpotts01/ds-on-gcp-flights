bq query --destination_table dsongcp.flights_sample `
--replace --nouse_legacy_sql `
'SELECT * FROM dsongcp.flights WHERE RAND() < 0.001'

bq extract --destination_format=NEWLINE_DELIMITED_JSON `
dsongcp.flights_sample `
gs://${BUCKET}/flights/ch4/flights_sample.json