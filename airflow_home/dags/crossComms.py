from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner' : 'The Engineer'
    }

dag = DAG(
    dag_id = 'CrossComms',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily'
)


def get_number(value):
    print("Value {value}!".format(value=value))
    return value + 1

def add_nine(ti):
    value=ti.xcom_pull(task_ids='get_number')
    print("Value {value}!".format(value=value))
    return value * 15

def print_val(ti):
    value=ti.xcom_pull(task_ids='add_nine')
    print("Value {value}!".format(value=value))


task1=PythonOperator(
    task_id = 'get_number',
    python_callable=get_number,
    op_kwargs={'value':20},
    dag=dag
)


task2=PythonOperator(
    task_id='add_nine',
    python_callable=add_nine,
    dag=dag
)

task3=PythonOperator(
    task_id='print_val',
    python_callable=print_val,
    dag=dag
)


task1 >> task2 >> task3