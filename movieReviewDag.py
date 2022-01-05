from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import *

#Dags
dag = DAG('MovieReviewLogicDAG', description='Moview review logic DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

# Tasks
command = "python /opt/airflow/dags/repo/movieReviewLogic.py --input gs://de-bootcamp-ag-raw/movie_review.csv --output gs://de-bootcamp-ag-stagin/movie_review/output --runner DataflowRunner --project de-bootcamp-ag --region us-central1  --temp_location gs://de-bootcamp-ag-stagin/tmp/"

runDataflowJob = BashOperator(task_id='runMovieReviewLogic', bash_command=command,dag = dag)

command = "gcloud auth application-default login"

getAppPermissions = BashOperator(task_id='getAppPermissions', bash_command=command,dag = dag)

getAppPermissions >> runDataflowJob
