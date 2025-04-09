URL_GREEN="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"
URL_ZONES="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

docker run -it \
    --network=homework_default \
    taxi_ingest:v001 \
    --user=postgres \
    --password=postgres \
    --host=postgres \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green_taxi_trips \
    --url_green=${URL_GREEN} \
    --url_zones=${URL_ZONES}


 