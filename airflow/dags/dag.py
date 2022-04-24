import datetime
import logging
import time
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


# imports for webscarping_task 
from bs4 import BeautifulSoup
import requests
from urllib.request import urlretrieve, quote
from urllib.parse import urljoin

# imports for decompressing_task
import pathlib
import tarfile

# imports for csv_to_parquet_task
import glob
import pyarrow.csv as pv
import pyarrow.parquet as pq

#imports for ingest_to_postgres_task
import pandas as pd
import psycopg2
import sql_queries

from webscraper import webscraper_callable
#from decompressing import decompressing_callable
from csv_to_parquet import csv_to_parquet_callable
from ingestion_to_postgres import ingest_to_postgres_callable


os.getenv("")
CSV_FILEPATH=""
for root, dirs, files in os.walk(os.getcwd()):
    # find all file in the path "filepath" then filters them by the expression '*..tar.zst'
    files= glob.glob(os.path.join(root,'*.tar.zst'))
    for file in files:
        filepath=file


sponsorblock_workflow = DAG(
    "sponsorblockDag",
    schedule_interval="0 6 * * *",# every day at 6am for more info check:https://crontab.guru/#0_6_*_*_*
    start_date=datetime.datetime.now()
)

with sponsorblock_workflow:
    web_scraping_task = PythonOperator(
        task_id='web_scraping',
        python_callable=webscraper_callable,
    )

    decompressing_task = BashOperator(
        task_id='decompressing',
        bash_command=f'zstd -d {filepath}'
    )
    extracting_tar_task = BashOperator(
        task_id='extracting_tar_file',
        bash_command=f'tar -xvf {filepath[:-4]} -C $AIRFLOW_HOME/data'
    )
    csv_to_parquet_task = PythonOperator(
        task_id="csv_to_parquet",
        python_callable=csv_to_parquet_callable,
    )

    ingest_to_postgres_task = PythonOperator(
        task_id="ingest_to_postgres",
        python_callable=ingest_to_postgres_callable,
        op_kwargs=dict(
            filepath=CSV_FILEPATH
        ),
    )

    dbt_task = BashOperator(
        task_id='dbt_task',
        bash_command='dbt run'
    )

    web_scraping_task >> decompressing_task >> extracting_tar_task >> csv_to_parquet_task >> ingest_to_postgres_task >> dbt_task