from datetime import datetime
from fileinput import filename

from numpy import load
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import *
from airflow.providers.google.cloud.transfers.local_to_gcs import *
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
idLog = []
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
def createDimensionTables():
    element = 0
    with open("user_purchase.csv", mode='r') as csv_file:
        userPurchase = csv.DictReader(csv_file)
        line_count = 0
        for row in userPurchase:
            if line_count == 0:
                line_count += 1 #Skip header
            else:
                invoiceNo.append(row["InvoiceNo"])
                stockCode.append(row["StockCode"])
                description.append(row["Description"])
                quantity.append(row["Quantity"])
                invoiceDate.append(row["InvoiceDate"])
                unitPrice.append(row["UnitPrice"])
                customerId.append(row["CustomerID"])
                country.append(row["Country"])

    with open("movieResults.csv", mode='r') as csv_file:
        movieReview = csv.DictReader(csv_file)
        line_count = 0
        for row in movieReview:
            if line_count == 0:
                line_count += 1 #Skip header
            else:
                cid.append(row["cid"])
                review.append(row["review"]) # Change for a proper header

    with open("logResults.csv", mode='r') as csv_file:
        userPurchase = csv.DictReader(csv_file)
        line_count = 0
        for row in userPurchase:
            if line_count == 0:
                line_count += 1 #Skip header
            else:
                idLog.append(row["id"])
                date.append(row["date"])
                device.append(row["device"])
                location.append(row["location"])
                os.append(row["os"])
                ip.append(row["ip"])
                telephone.append(row["telephone"])
                browser.append("") # Workaround for the moment 

    with open('dim_browser.csv', 'w', encoding='UTF8') as f:
        header = ['id_dim_browser', 'browser']
        writer = csv.writer(f)
        writer.writerow(header)
        for i in idLog:
            data = [i,""] #Onlypri in browser
            print (data)
            writer.writerow(data)
            element += 1

    with open('dim_os.csv', 'w', encoding='UTF8') as f:
        header = ['id_dim_os', 'os']
        writer = csv.writer(f)
        writer.writerow(header)
        for i in idLog:        
            data = [i,os[element]] #Onlypri in browser
            writer.writerow(data)
            element += 1
    element = 0

    with open('dim_location.csv', 'w', encoding='UTF8') as f:
        header = ['id_dim_location', 'location']
        writer = csv.writer(f)
        writer.writerow(header)
        for i in idLog:            
            data = [i,location[element]] #Onlypri in browser
            writer.writerow(data)
            element += 1
    element = 0

    with open('dim_devices.csv', 'w', encoding='UTF8') as f:
        header = ['id_dim_devices', 'device']
        writer = csv.writer(f)
        writer.writerow(header)
        for i in idLog:
            data = [i,device[element]] #Onlypri in browser
            writer.writerow(data)
            element += 1
    element = 0

    with open('dim_date.csv', 'w', encoding='UTF8') as f:
        header = ['id_dim_date', 'log_date','day','month','year','season']
        writer = csv.writer(f)
        writer.writerow(header)
        for i in idLog:
            splitDate = date[element].split("-")
            data = [i,date[element],splitDate[0],splitDate[1],splitDate[2],""] #Onlypri in browser
            writer.writerow(data)
            element += 1
    element = 0
    '''
    with open('dim_fact_movie_analytics.csv', 'w', encoding='UTF8') as f:
        header = ['customerId','id_dim_devices','id_dim_location','id_dim_os','id_dim_browser','amount_spent','review_score','review_count','insert_date']
        writer = csv.writer(f)
        writer.writerow(header)
        for i in customerId:
            data = [i,device[element]] #Onlypri in browser
            writer.writerow(data)
            element += 1
    element = 0
    '''
# Tasks

createDimensionTablesTask = PythonOperator(task_id='create_dimension_tables', python_callable=createDimensionTables, dag=dag)

readUserPurchaseFile  = GCSToLocalFilesystemOperator(
        task_id="getUserPurchaseFromBucket",
        object_name="user_purchase.csv",
        bucket="de-bootcamp-ag-staging",
        filename="user_purchase.csv",
        dag=dag
    )

readMoviewReviewFile  = download_file = GCSToLocalFilesystemOperator(
        task_id="getMovieReviewFromBucket",
        object_name="results/movie_review-00000-of-00001",
        bucket="de-bootcamp-ag-staging",
        filename="movieResults.csv",
        dag=dag
    )

readLogReviewFile  = download_file = GCSToLocalFilesystemOperator(
        task_id="getLogReviewFromBucket",
        object_name="results/log_review_output-00000-of-00001",
        bucket="de-bootcamp-ag-staging",
        filename="logResults.csv",
        dag=dag
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

loadDimensionBrowserTable  = GoogleCloudStorageToBigQueryOperator(
    task_id='loadDimensionBrowserTable',
    bucket='de-bootcamp-ag-staging',
    source_objects=['dimensionTables/dim_browser.csv'],
    destination_project_dataset_table='de_bootcamp_ag_dataset.dim_browser',
    schema_fields=[
        {'name': 'id_dim_browser', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'browser', 'type': 'STRING', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows = 1,
    dag=dag)


uploadDimensionBrowserTable = LocalFilesystemToGCSOperator(
        task_id="uploadDimensionBrowserTable",
        src="dim_browser.csv",
        dst="dimensionTables/",
        bucket="de-bootcamp-ag-staging",
        dag=dag
    )
#loadUserPurchaseIntoBigquery >> loadMovieReviewIntoBigquery >> loadLogReviewIntoBigquery
readUserPurchaseFile >> readMoviewReviewFile >> readLogReviewFile >>  createDimensionTablesTask >> uploadDimensionBrowserTable >> loadDimensionBrowserTable
