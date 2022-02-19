from datetime import datetime
from fileinput import filename

from numpy import load
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import *
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.operators.gcs_to_bq import *
from google.cloud import storage
from google.cloud.storage import blob
import psycopg2
import csv

#User Purchase
invoiceNo = []
stockCode = []
description = []
quantity = []
invoiceDate = []
unitPrice = []
customerId = []
country = []

#Movie Review
cid = []
review = []

#Log Review  id,date,device,location,os,ip,telephone
id = []
date = []
device = []
location = []
os = []
ip = []
telephone = []
browser = []
#DAG

dag = DAG('LoadIntoBigQuery', description='Upload CSV from google storage to bigquery',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

# Functions
# Headers InvoiceNo	StockCode	Description	Quantity	InvoiceDate	UnitPrice	CustomerID	Country
def preprocessUserPurchase():
    with open("user_purchase.csv", mode='r') as csv_file:
        userPurchase = csv.DictReader(csv_file)
        line_count = 0
        for row in userPurchase:
            if line_count == 0:
                line_count += 1 #Skip header
            invoiceNo.append(row["InvoiceNo"])
            stockCode.append(row["stockCode"])
            description.append(row["Description"])
            quantity.append(row["Quantity"])
            invoiceDate.append(row["InvoiceDate"])
            unitPrice.append(row["UnitPrice"])
            customerId.append(row["CustomerID"])
            country.append(row["Country"])

def preprocessMovieReview():
    with open("movieResults.csv", mode='r') as csv_file:
        movieReview = csv.DictReader(csv_file)
        line_count = 0
        for row in movieReview:
            if line_count == 0:
                line_count += 1 #Skip header
            cid.append(row["cid"])
            review.append(row["0"]) # Change for a proper header

def preprocessLogReview():
    with open("LogResults.csv", mode='r') as csv_file:
        userPurchase = csv.DictReader(csv_file)
        line_count = 0
        for row in userPurchase:
            if line_count == 0:
                line_count += 1 #Skip header
            id.append(row["id"])
            date.append(row["date"])
            device.append(row["device"])
            location.append(row["location"])
            os.append(row["os"])
            ip.append(row["ip"])
            telephone.append(row["telephone"])
            browser.append("") # Workaround for the moment 

# Tasks

preprocessUserPurchaseTask = PythonOperator(task_id='preprocess_user_purchase', python_callable=preprocessUserPurchase, dag=dag)
preprocessMovieReviewTask = PythonOperator(task_id='preprocess_moview_review', python_callable=preprocessMovieReview, dag=dag)
preprocessLogReviewTask = PythonOperator(task_id='preprocess_log_review', python_callable=preprocessLogReview, dag=dag)

readUserPurchaseFile  = GCSToLocalFilesystemOperator(
        task_id="getUserPurchaseFromBucket",
        object_name="user_purchase.csv",
        bucket="de-bootcamp-ag-staging",
        filename="user_purchase.csv",
    )

readMoviewReviewFile  = download_file = GCSToLocalFilesystemOperator(
        task_id="getMovieReviewFromBucket",
        object_name="results/movie_review-00000-of-00001",
        bucket="de-bootcamp-ag-staging",
        filename="movieResults.csv",
    )

readLogReviewFile  = download_file = GCSToLocalFilesystemOperator(
        task_id="getLogReviewFromBucket",
        object_name="results/log_review-00000-of-00001",
        bucket="de-bootcamp-ag-staging",
        filename="LogResults.csv",
    )

#Operators 

loadUserPurchaseIntoBigquery  = GoogleCloudStorageToBigQueryOperator(
    task_id='loadUserPurchase',
    bucket='de-bootcamp-ag-staging',
    source_objects=['user_purchase.csv'],
    destination_project_dataset_table='de_bootcamp_ag_dataset.tempUserPurchase',
    schema_fields=[
        {'name': 'InvoiceNo', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'StockCode', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Description', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Quantity', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'InvoiceDate', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'UnitPrice', 'type': 'DECIMAL', 'mode': 'NULLABLE'},
        {'name': 'CustomerId', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'Country', 'type': 'STRING', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows = 1,
    dag=dag)

loadMovieReviewIntoBigquery  = GoogleCloudStorageToBigQueryOperator(
    task_id='loadMoviewReview',
    bucket='de-bootcamp-ag-staging',
    source_objects=['results/movie_review-00000-of-00001'],
    destination_project_dataset_table='de_bootcamp_ag_dataset.tempMoviewReview',
    schema_fields=[
        {'name': 'cid', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'review', 'type': 'NUMERIC', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows = 1,
    dag=dag)

loadLogReviewIntoBigquery  = GoogleCloudStorageToBigQueryOperator(
    task_id='loadLogReview',
    bucket='de-bootcamp-ag-staging',
    source_objects=['results/log_review_output-00000-of-00001'],
    destination_project_dataset_table='de_bootcamp_ag_dataset.tempLogReview',
    schema_fields=[
        {'name': 'id', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'device', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'location', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'os', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ip', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'telephone', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows = 1,
    dag=dag)



#loadUserPurchaseIntoBigquery >> loadMovieReviewIntoBigquery >> loadLogReviewIntoBigquery
readUserPurchaseFile >> readMoviewReviewFile >> readLogReviewFile >> preprocessUserPurchaseTask >> preprocessMovieReviewTask >> preprocessLogReviewTask
