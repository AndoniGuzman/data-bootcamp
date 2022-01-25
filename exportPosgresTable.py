from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import *
from airflow.providers.google.cloud.transfers.local_to_gcs import *
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

def exportTable():
    hook = PostgresHook()
    hook.bulk_dump(tableName, filename)
        
# Tasks

exportTable = PythonOperator(task_id='exportTableToFile', python_callable=exportTable, dag=dag)

upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_table_to_staging",
        src=filename,
        dst="/",
        bucket=bucket,
    )

exportTable >> upload_file
