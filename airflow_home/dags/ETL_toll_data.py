from datetime import timedelta
from airflow import DAG
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner':'The Engineer',
    'start_date':days_ago(0),
    'email':['kennethnebe@gmail.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
}

dag = DAG(
    'ETL_toll_data',
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description='Apache Airflow tolldata',
)


def consolidate():
    csv_data = pd.read_csv('/home/engineer/project/tolldata/cleaned/csv_data.csv')
    fixed_data = pd.read_csv('/home/engineer/project/tolldata/cleaned/fixed_width_data.csv')
    tsv_data = pd.read_csv('/home/engineer/project/tolldata/cleaned/tsv_data.csv')
    merged = pd.concat([csv_data, fixed_data, tsv_data], axis=1)
    merged.to_csv("/home/engineer/project/tolldata/cleaned/extracted_data.csv", index=False, header=False)
    
def load_to_postgres():
    engine = create_engine("postgresql+psycopg2://airflow_user:airflow_pass@localhost:5432/tolldata")
    columns = ["id", "timestamp", "vehicle_number", "vehicle_type", "type", "code", "vin", "tin", "plate"]
    df = pd.read_csv("/home/engineer/project/tolldata/cleaned/extracted_data.csv", header=None, names=columns)
    df.to_sql("car_details", engine, if_exists="replace", index=False)

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xvf /home/engineer/project/tolldata.tgz -C /home/engineer/project/tolldata',
    dag=dag,
)

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command="cut -d',' -f1-4 /home/engineer/project/tolldata/vehicle-data.csv > /home/engineer/project/tolldata/cleaned/csv_data.csv",
    dag=dag,
)

#note pipe in tr -d '\r' to remove trailing newline
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="cut -d$'\t' -f5,6,7 /home/engineer/project/tolldata/tollplaza-data.tsv |tr -d '\r' |tr '\t' ',' > /home/engineer/project/tolldata/cleaned/tsv_data.csv",
    dag=dag,
)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="cut -c59-62,63-67 /home/engineer/project/tolldata/payment-data.txt | tr ' ' ',' > /home/engineer/project/tolldata/cleaned/fixed_width_data.csv",
    dag=dag,
)

consolidate_data = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate,
    dag=dag,
)

postgresload = PythonOperator(
    task_id='postgresload',
    python_callable=load_to_postgres,
    dag=dag,
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> postgresload