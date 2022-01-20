from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import *
from airflow.hooks.postgres_hook import PostgresHook

from google.cloud import storage
from google.cloud.storage import blob
import psycopg2

#DAG
dag = DAG('DataUploadToPostgreSQL', description='Read a csv and upload it to a postgresSQL',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

# Functions
def readFileFromBucket(bucketName, fileName):
    storageClient = storage.Client()
    bucket = storageClient.bucket(bucketName)
    blob = bucket.blob(fileName)

    blob.download_to_filename("test.csv")  

def uploadDataIntoDatabase():
    hook = PostgresHook()
    databaseConnection = hook.get_conn()
    cursor = databaseConnection.cursor()

    table = "user_purchase"
    cursor.execute("truncate " + table + ";")
    
    with open('test.csv','r') as file:
       next(file)
       cursor.copy_expert("""COPY user_purchase FROM STDIN WITH (FORMAT CSV)""", file)
    '''   
        cursor.copy_from(file,"user_purchase",sep=',',columns=('invoice_number',
                                                               'stock_code',
                                                               'detail',
                                                               'quantity',
                                                               'invoice_date',
                                                               'unit_price',
                                                               'customer_id',
                                                               'country'))
    '''
    databaseConnection.commit()
    databaseConnection.close()

# Tasks

command = "pip install bs4 ;pip install psycopg2-binary ; pip install google-cloud-storage; pip install wheel; pip3 install 'apache-beam[gcp]'"
installPipDependencies = BashOperator(task_id='installPipDependencies', bash_command=command,dag = dag)

readFile = download_file = GCSToLocalFilesystemOperator(
        task_id="getFileFromBucket",
        object_name="user_purchase.csv",
        bucket="de-bootcamp-ag",
        filename="test.csv",
    )

uploadToDatabase = PythonOperator(task_id='uploadDataToDatabase', python_callable=uploadDataIntoDatabase, dag=dag)

installPipDependencies >> readFile >> uploadToDatabase
