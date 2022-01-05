from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import *


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
