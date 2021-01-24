import os

from dotenv import load_dotenv
load_dotenv()

import sys
sys.path.append(f"{os.environ['AIRFLOW_HOME']}/apps_functions/twitter")

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import twitter


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}


dag = DAG(
    dag_id='tweets_by_user',
    default_args=default_args,
    description='Query up to last 3000 tweets by user in a list',
    schedule_interval='*/1 * * * *',
    start_date=datetime(2021, 1, 19 , 22, 39),
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
        op_kwargs={'userlist': ['bolsonarosp', 'opropriolavo']},
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