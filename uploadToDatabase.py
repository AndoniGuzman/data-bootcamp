from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

import logging 
import os 
from google.cloud import storage
from google.cloud.storage import blob
import psycopg2


# Functions

def readFileFromBucket(bucketName, fileName):
    storageClient = storage.Client()
    bucket = storageClient.bucket(bucketName)
    blob = bucket.blob(fileName)

    blob.download_to_filename("test.csv")
    

   
def databaseConnection(databaseName,user,password,host,port):
    connection = psycopg2.connect(database=databaseName,
                                  user = user, password = password,
                                  host = host , port = port  )

    print ("Connection succefull")            
    #connection.close()
    return connection

def uploadDataIntoDatabase(databaseConnection):
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

def print_hello():
    return 'Hello world from first Airflow DAG!'

#Dags
dag = DAG('DataUploadToPostgreSQL', description='Read a csv and upload it to a postgresSQL',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

#hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

# Tasks


installPipDependencies = BashOperator(task_id='installPipDependencies', bash_command="pip install psycopg2-binary ; pip install google-cloud-storage",
                                      dag = dag)

readFile = PythonOperator(task_id='getFileFromBucket', python_callable = readFileFromBucket, dag = dag)


installPipDependencies >> readFile 
