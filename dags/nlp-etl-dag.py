# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

default_config = {
    'competition_name': 'jigsaw-multilingual-toxic-comment-classification'
}

dag = DAG('nlp-etl',
          default_args=default_args,
          description='Kaggle NLP ETL pipeline',
          schedule_interval=timedelta(days=1),
          start_date=days_ago(2),
          tags=['kaggle', 'nlp', 'etl'])

dag.params = default_config


def t2_func(**context):
    print("this is task-2.")


with dag:
    t1 = BashOperator(
        task_id='download_dataset',
        bash_command=
        'kaggle competitions download {{ dag_run.conf["competition_name"]}} -p /tmp'
    )
    t2 = PythonOperator(task_id='task2', python_callable=t2_func)
    t1 >> t2
