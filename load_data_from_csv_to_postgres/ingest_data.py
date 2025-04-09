"""
Loads data from csv files to our Postgres DB
"""

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url_green = params.url_green
    url_zones = params.url_zones

    # LOAD GREEN TAXI DATA FIRSTs

    # The backup files are gzipped, and it's important to keep the correct 
    # extension for Pandas to be able to open the file
    if url_green.endswith('.csv.gz'):
        csv_name = 'greentaxis.csv.gz'
    else:
        csv_name = 'greentaxis.csv'

    os.system(f"wget {url_green} -O {csv_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    # Create an iterator to load the CSV
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=1e5)
    df = next(df_iter)

    # Set columns to the proper format
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    # Set table schema in Postgres
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    
    # Load data
    df.to_sql(name=table_name, con=engine, if_exists='append')
    while True:
        try:
            t_start = time()
            df = next(df_iter)

            # Set columns to the proper format
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print(f"Inserted another chunk, took {t_end - t_start}.3f seconds")
        
        except StopIteration:
            print("Finished ingesting data into Postgres")
            break

    # READ TAXI ZONES DATA
        
    if url_zones.endswith('.csv.gz'):
        csv_name = 'taxizones.csv.gz'
    else:
        csv_name = 'taxizones.csv'

    os.system(f"wget {url_zones} -O {csv_name}")
    
    df_zones = pd.read_csv(csv_name)
    df_zones.to_sql(name='zones', con=engine, if_exists='replace')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='Username for Postgres')
    parser.add_argument('--password', required=True, help='Password for Postgres')
    parser.add_argument('--host', required=True, help='Hostname for Postgres')
    parser.add_argument('--port', required=True, help='Port for Postgres')
    parser.add_argument('--db', required=True, help='Database name for postgres')
    parser.add_argument('--table_name', required=True, help='Name of the table where we will write the results')
    parser.add_argument('--url_green', required=True, help='URL of the green taxi data file')
    parser.add_argument('--url_zones', required=True, help="URL of the taxi zones data file")

    args = parser.parse_args()

    main(args)

