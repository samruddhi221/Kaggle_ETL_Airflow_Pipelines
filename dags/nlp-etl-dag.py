# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from kaggle_etl import dataloader_factory
from kaggle_etl.dataloader_factory import DataloaderFactory

import os
import pandas as pd

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


def dataloader_helper(competition_name):
    dataloader_fn = DataloaderFactory(competition_name).get_loader()
    out_dicts = dataloader_fn(os.path.join('/tmp', competition_name))
    out_dicts['train'].to_hdf(
        os.path.join('/tmp', competition_name, 'train.h5'), key='df', mode='w')
    out_dicts['validation'].to_hdf(
        os.path.join('/tmp', competition_name, 'validation.h5'), key='df', mode='w')
    out_dicts['test'].to_hdf(
        os.path.join('/tmp', competition_name, 'test.h5'), key='df', mode='w')
    print("----------------------------------------------")
    test_df = pd.read_hdf(os.path.join('/tmp', competition_name, 'test.h5'), 'df') 
    print(test_df.head())


with dag:
    download_dataset = BashOperator(
        task_id='download_dataset',
        bash_command=
        'kaggle competitions download {{ dag_run.conf["competition_name"]}} -p /tmp'
    )
    extract_dataset = BashOperator(
        task_id='extract_dataset',
        bash_command=
        'mkdir -p /tmp/{{ dag_run.conf["competition_name"]}} && unzip /tmp/{{dag_run.conf["competition_name"]}}.zip -d /tmp/{{ dag_run.conf["competition_name"]}}'
    )
    create_df = PythonOperator(
        task_id='create_dataframes',
        python_callable=dataloader_helper,
        op_args=['{{dag_run.conf["competition_name"]}}'])
    download_dataset >> extract_dataset >> create_df
