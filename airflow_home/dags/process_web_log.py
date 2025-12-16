from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator


default_args = {
    'owner':'Onyinyechukwu Kenneth Nebe',
    'start_date':days_ago(0),
    'email':['kennethnebe@gmail.com'],
    'retries':1,
    'retry_delay':timedelta(minutes=5),
}


dag = DAG(
    dag_id='process_web_log',
    default_args=default_args,
    description='ETL Web Access Log Processing Pipeline',
    schedule_interval=timedelta(days=1),
)

extract_data = BashOperator(
        task_id='extract_data',
        bash_command='cut -d " " -f1 $AIRFLOW_HOME/dags/capstone/accesslog.txt > $AIRFLOW_HOME/dags/capstone/extracted_data.txt',
        dag=dag
    )

transform_data = BashOperator(
        task_id='transform_data',
        bash_command='grep -v "198.46.149.143" $AIRFLOW_HOME/dags/capstone/extracted_data.txt > $AIRFLOW_HOME/dags/capstone/transformed_data.txt',
        dag=dag
    )

load_data = BashOperator(
        task_id='load_data',
        bash_command='tar -cvf $AIRFLOW_HOME/dags/capstone/weblog.tar $AIRFLOW_HOME/dags/capstone/transformed_data.txt'
    )


extract_data >> transform_data >> load_data