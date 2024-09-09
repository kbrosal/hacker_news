from airflow import models
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2024, 8, 24),
    'retries': 1,
    'retry_delay': timedelta(seconds=50),
    'dataflow_default_options': {
        'project': 'data-engineering-433013',
        'region': 'asia-southeast1',
        'runner': 'DataflowRunner',
    }
}

with models.DAG(
    'hacker_news_dag',
    default_args = default_args,
    schedule_interval = timedelta(days=1),
    catchup = False
) as dag:

    dataflow_task = DataflowCreatePythonJobOperator(
        task_id ='beamTask',
        py_file ='gs://beam_hnews/beam_pipeline.py', 
        location ='asia-southeast1',
        project_id ='data-engineering-433013',
        gcp_conn_id ='google_cloud_default',
        job_name ='beam_pipelineJob',
        temp_location ='gs://beam_hnews/airflow/temp',  
        staging_location ='gs://beam_hnews/airflow/staging'  
    )

