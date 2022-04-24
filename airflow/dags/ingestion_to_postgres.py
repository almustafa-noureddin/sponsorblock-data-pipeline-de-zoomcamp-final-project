import os
import glob
import logging
from time import time

import pandas as pd
import psycopg2
import pyarrow.csv as pv
import pyarrow.parquet as pq

import sql_queries

# gets database credentials from .env file
PG_HOST =os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE =os.getenv('PG_DATABASE')
PG_DATABASE_TEMP = os.getenv('PG_DATABASE_TEMP')
DBT_RAW_DATA=os.getenv('DBT_RAW_DATA')
DBT_FINAL_DATA=os.getenv('DBT_FINAL_DATA')
FILEPATH=os.getcwd()

def create_database(user, password , host, port, db, db_temp, schema_raw_data, schema_final_data):
    '''Creates and connects to sponsorblockdb database. Returns cursor and connection to DB
    dp_temp is any permenant database that lives in your system, its only function is to connect
    to postgres, create a new database then switch to the newly created database. 
    '''
    # connect to default database
    logging.info(f'connecting to the database {db_temp}')
    try:
        conn = psycopg2.connect(f"host={host} dbname={db_temp} user={user} password={password} port={port}")
        conn.set_session(autocommit=True)
        cur = conn.cursor()
    except psycopg2.Error as e: 
        logging.info(f"Error: Could not make connection to the database {db_temp}")
        raise e
    
    logging.info(f'successfully connected to the database {db_temp}')
    
    # create sponsorblockdb database with UTF8 encoding
    logging.info(f'droping database {db}')
    try:
        cur.execute(f"DROP DATABASE IF EXISTS {db}")
    except psycopg2.Error as e:
        logging.info(f'Error: Could not drop the database {db}')
        raise e
    logging.info(f'droped database {db}')
    
    logging.info(f'creating new database {db}')
    try:
        cur.execute(f"CREATE DATABASE {db} WITH ENCODING 'utf8'")
    except psycopg2.Error as e:
        logging.info(f'Error: Could not create the database f{db}')
        raise e
    logging.info(f'successfully created new database {db}')

    # close connection to default database
    logging.info(f'closing connection to the database {db_temp}')
    conn.close()    
    logging.info(f'successfully closed connection to the database {db_temp}')
    
    # connect to the new database
    logging.info(f'connecting to the database {db}')
    try:
        conn = psycopg2.connect(f"host={host} dbname={db} user={user} password={password} port={port}")
    except psycopg2.Error as e: 
        logging.info(f"Error: Could not make connection to f{db}")
        raise e
    logging.info(f'successfully connected to the database {db}')
    cur = conn.cursor()
    cur.execute(f"CREATE SCHEMA {schema_raw_data} AUTHORIZATION {user}")
    cur.execute(f"CREATE SCHEMA {schema_final_data} AUTHORIZATION {user}")
    cur.execute(f"GRANT ALL ON SCHEMA {schema_raw_data} TO {user}")
    return cur, conn


def drop_tables(cur, conn):
    '''Drops all tables created on the database'''
    logging.info('Droping all tables')
    for query in sql_queries.drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            logging.info('Error: Could not drop the tables')
            raise e
    logging.info('successfully Droped all tables')


def create_tables(cur, conn):
    '''Created tables defined on the sql_queries'''
    logging.info('creating new tables')
    for query in sql_queries.create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            logging.info('Error: Could not create the tables')
            raise e
    logging.info('successfully created new tables')


def insert_data(filenumber,cur,conn,parquet_file,table_name):
    ''' Insert parquet files from data lake to the database '''
    logging.info(f'inserting data to {table_name}')
    logging.info('connection established successfully, inserting data...')
    t_start = time()
    df=pd.read_parquet(parquet_file)
    for i, row in df.iterrows():
        try:
            cur.execute(sql_queries.insert_table_queries[filenumber-1], list(row))
        except psycopg2.Error as e:
            logging.info(f'Error: Could not insert data to the table {table_name}')
            raise e
    t_end = time()
    logging.info('data is inserted successfully took %.3f second' % (t_end - t_start))

def get_files_and_insert_data(cur,conn,filepath,insert_function):
    ''' Gets parquet file paths and iterate through each of them perfoming insert queries
    using data insert function
    '''
    all_files = []
    file_names = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.parquet'))
        for file in files :
            all_files.append(os.path.abspath(file))
            file_names.append(file[len(root)+1:-8])

    # get total number of files found
    num_files = len(all_files)
    logging.info(f'{num_files} files found in {filepath}')

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        insert_function(i, cur, conn, datafile, table_name=file_names[i-1])
        conn.commit()
        logging.info(f'{i}/{num_files} files ingested to {PG_DATABASE}.')
    return all_files

def ingest_to_postgres_callable():
    cur, conn = create_database(
            PG_USER,
            PG_PASSWORD,
            PG_HOST,
            PG_PORT,
            PG_DATABASE,
            PG_DATABASE_TEMP,
            DBT_RAW_DATA,
            DBT_FINAL_DATA
            )
    drop_tables(cur, conn)
    create_tables(cur, conn)
    get_files_and_insert_data(cur, conn, FILEPATH, insert_data)

    conn.close()

def main():
    ''' Function to drop and re create sponsorblockdb database and all related tables
    then ingest data in them.
    '''
    ingest_to_postgres_callable()

if __name__ == "__main__":
    main()