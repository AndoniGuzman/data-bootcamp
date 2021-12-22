from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


def print_hello():
    return 'Hello world from first Airflow DAG!'

dag = DAG('DataUploadToPostgreSQL', description='Read a csv and upload it to a postgresSQL',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

#hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)
installPipDependencies = BashOperator(task_id='installPipDependencies', bash_command="pip install psycopy2-binary ; pip install google-cloud-storage",
                                      dag = dag)

installPipDependencies
