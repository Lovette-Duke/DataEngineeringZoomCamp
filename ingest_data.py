#!/usr/bin/env python
#coding: utf-8

# pip install sqlalchemy
# pip install psycopg2

import requests
import argparse
from time import time
from sqlalchemy import create_engine
import pandas as pd




def main(params):
    user = params.user
    password = params.password
    port = params.port
    host = params.host
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = 'output.csv'

    #os.system(f"wget {url} -o {csv_name}")
    response = requests.get(url) 
    with open(csv_name, 'wb') as file: 
        file.write(response.content)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    df_iter = pd.read_csv(csv_name, iterator=True, compression='gzip', chunksize=100000, low_memory=False, encoding='latin1')

    df = next(df_iter)
    print(df.head())
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime']) 
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True: 
        try: 
            t_start = time()
            
            df = next(df_iter) 
            df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime']) 
            df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime']) 
            
            df.to_sql(name=table_name, con=engine, if_exists='append')
            
            t_end = time() 
            
            print('Inserted another chunk... took %.3f seconds' % (t_end - t_start)) 
        except StopIteration: 
            print('Completed') 
            break
        except Exception as e: 
            print(f'Error: {e}') 
            break

# print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Populate csv file to Postgres')

    parser.add_argument('--user', help='username for postgres')   # positional argument
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host name for postgress')
    parser.add_argument('--db', help='database name for postgress')
    parser.add_argument('--port', help='connection port for postgres')
    parser.add_argument('--table_name', help='name of the table we will write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)


