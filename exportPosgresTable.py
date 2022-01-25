from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import *
from airflow.hooks.postgres_hook import PostgresHook

from google.cloud import storage
from google.cloud.storage import blob
import psycopg2

tableName = "user_purchase" # See the way to get it from other place 
filename = tableName + ".csv"
bucket = "de-bootcamp-ag-staging"

#DAG
dag = DAG('ExportPostgresTable', description='Export a table from postgres ',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

# Functions

def exportTable(tableName):
    filename = tableName + ".csv"
    with open(filename) as file:
        hook = PostgresHook()
        hook.bulk_dump(tableName, file)
        
# Tasks

exportTable = PythonOperator(task_id='exportTableToFile', python_callable=exportTable(tableName), dag=dag)

upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_table_to_staging",
        src=filename,
        dst="/",
        bucket=bucket,
    )

exportTable >> upload_file
