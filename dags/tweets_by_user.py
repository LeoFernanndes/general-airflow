import os

from dotenv import load_dotenv
load_dotenv()

import sys
sys.path.append(f"{os.environ['AIRFLOW_HOME']}/apps_functions/twitter")

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import twitter


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}


dag = DAG(
    dag_id='tweets_by_user',
    default_args=default_args,
    description='Query up to last 3000 tweets by user in a list',
    schedule_interval='* * * * *',
    start_date=datetime(2021, 2, 18, 22, 25),
    tags=['example'],
)


get_userlist = PythonOperator(
    task_id='get_userlist',
    python_callable=twitter.get_userlist,
    dag=dag
    )


get_tweets_by_user_list = PythonOperator(
        task_id='get_tweets_by_user_list',
        python_callable=twitter.tweets_by_userlist,
        op_kwargs={},
        dag=dag
    )


def print_userlist(**context):
    userlist = context['task_instance'].xcom_pull(task_ids='get_userlist')['userlist']
    print(userlist)
    

print_userlist_task = PythonOperator(
        task_id='print_userlist_task',
        python_callable=print_userlist,
        # op_kwargs={'userlist': userlist}
    )
    

get_userlist >> get_tweets_by_user_list >> print_userlist_task