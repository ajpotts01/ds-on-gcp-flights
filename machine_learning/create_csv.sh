BUCKET=ajp-ds-gcp-flights
PROJECT=ajp-ds-gcp
for dataset in "train" "eval" "all"; do
  TABLE=dsongcp.flights_${dataset}_data
  CSV=gs://${BUCKET}/ch9/data/${dataset}.csv
  echo "Exporting"
  bq.cmd --project_id=${PROJECT} extract --destination_format=CSV $TABLE $CSV
done