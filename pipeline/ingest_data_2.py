#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
import click

dtype = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string",
}

import click

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')

def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table):
    pg_user = pg_user
    pg_pass = pg_pass
    pg_host = pg_host
    pg_port = pg_port
    pg_db = pg_db

    target_table = 'zones'

    url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    df = pd.read_csv(
        url,
        dtype=dtype,
    )

    first = True


    if first:
        df.head(0).to_sql(
            name=target_table,
            con=engine,
            if_exists='replace'
        )
    first = False

    df.to_sql(
        name=target_table,
        con=engine, 
        if_exists='append'
    )

if __name__ == '__main__':
    run()