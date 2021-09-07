# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

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
    # download_dataset = BashOperator(
    #     task_id='download_dataset',
    #     bash_command=
    #     'kaggle competitions download {{ dag_run.conf["competition_name"]}} -p /tmp'
    # )
    dummy_op1 = DummyOperator(task_id='dummy_operator')
    extract_dataset = BashOperator(
        task_id='extract_dataset',
        bash_command=
        'mkdir -p /tmp/{{ dag_run.conf["competition_name"]}} && unzip /tmp/{{dag_run.conf["competition_name"]}}.zip -d /tmp/{{ dag_run.conf["competition_name"]}}'
    )
    dummy_op1 >> extract_dataset
