from datetime import datetime
import sys
import os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
sys.path.append('/usr/local/lib/python3.7/site-packages/pip/')
sys.path.append('/usr/local/lib/python3.7/site-packages/')
sys.path.append('/home/airflow/.local/lib/python3.7/')


from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import PythonVirtualenvOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
#from airflow.providers.apache.beam.operators.beam import *

#from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
#from airflow.providers.apache.beam.hooks.beam import *

from google.cloud import storage
from google.cloud.storage import blob

def readFileFromBucket(bucketName, fileName):
    storageClient = storage.Client()
    bucket = storageClient.bucket(bucketName)
    blob = bucket.blob(fileName)

    blob.download_to_filename("test.csv")

#Dags
dag = DAG('MovieReviewLogicDAG', description='Moview review logic DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)



# Tasks
#Test

movieReview = BeamRunPythonPipelineOperator(
    task_id="moviewReview",
    py_file="/opt/airflow/dags/repo/movieReviewLogic.py",
    py_options=[],
    pipeline_options={
        'output': "gs://de-bootcamp-ag-stagin/results/movieReview/output",
    },
    py_requirements=['apache-beam[gcp]>=2.21.0'],
    py_interpreter='python3',
    py_system_site_packages=False,
    dataflow_config={'location': 'us-central1',
                     'project_id': 'de-bootcamp-ag'}
)

movieReview
