from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import requests


#Define variables
input_file = 'web-server-access-log.txt'
extracted_file = 'webserverextract.txt'
transformed_file = 'webservertransform.txt'
output_file = 'captilized.txt'

def download_file():
    url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"
    
    # Send a GET request to the URL
    with requests.get(url, stream=True) as response:
        # Raise an exception for HTTP errors
        response.raise_for_status()
        
        # Open a local file in binary write mode
        with open(input_file, 'wb') as file:
            # Write the content to the local file in chunks
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

    print(f"File downloaded successfully: {input_file}")


def extract():
    global input_file
    print("Inside Extract")
    # Read the contents of the file into a string
    with open(input_file, 'r') as infile, \
            open(extracted_file, 'w') as outfile:
        for line in infile:
            fields = line.split('#')
            if len(fields) >= 6:
                field_1 = fields[0]
                field_4 = fields[3]
                outfile.write(field_1 + "#" + field_4 + "\n")
def transform():
    global extracted_file, transformed_file
    print("Inside Transform")
    with open(extracted_file, 'r') as infile, \
            open(transformed_file, 'w') as outfile:
        for line in infile:
            processed_line = line.lower()
            outfile.write(processed_line + '\n')
def load():
    global transformed_file, output_file
    print("Inside Load")
    # Save the array to a txt
    with open(transformed_file, 'r') as infile, \
            open(output_file, 'w') as outfile:
        for line in infile:
            outfile.write(line + '\n')
def check():
    global output_file
    print("Inside Check")
    # Save the array to a txt
    with open(output_file, 'r') as infile:
        for line in infile:
            print(line)

default_args = {
    'owner':'Onyinyechukwu Kenneth Nebe',
    'start_date':days_ago(0),
    'email':['kennethnebe@gmail.com'],
    'retries':1,
    'retry_delay':timedelta(minutes=5),
}

dag = DAG(
    'ETL_Server_Access_Log_Processing',
    default_args=default_args,
    description='My second DAG',
    schedule_interval=timedelta(days=1),
)

# Define the task named download to call the `download_file` function
download = PythonOperator(
    task_id='download',
    python_callable=download_file,
    dag=dag,
)

# Define the task named execute_extract to call the `extract` function
execute_extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)
# Define the task named execute_transform to call the `transform` function
execute_transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag,
)
# Define the task named execute_load to call the `load` function
execute_load = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
)
# Define the task named execute_load to call the `load` function
execute_check = PythonOperator(
    task_id='check',
    python_callable=check,
    dag=dag,
)
# Task pipeline
download >> execute_extract >> execute_transform >> execute_load >> execute_check