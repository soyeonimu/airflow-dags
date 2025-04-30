import pendulum
from airflow import DAG
from airflow.decorators import task
import pandas as pd
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryGetDataOperator
)
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

default_args = dict(
    owner = 'jaechanjo',
    email = ['jaechanjo@airflow.com'],
    email_on_failure = False,
    retries = 3
)

project_id = 'proven-octane-456123-s1'
dataset_id = 'airflow'
location = 'asia-northeast3'
bigquery_conn_name = 'bigquery_connection'

with DAG(
    dag_id="15_bigquery_operator_dag",
    start_date=pendulum.datetime(2024, 11, 10, tz='Asia/Seoul'),
    schedule="30 10 * * *", # cron 표현식
    tags = ['20250224'],
    default_args = default_args,
    catchup=False
) as dag:  # ✅ 반드시 `as dag` 로 선언해야 함

    # bigquery에 airflow라는 이름의 새로운 dataset을 생성!
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_dataset',
        project_id=project_id,
        dataset_id=dataset_id,
        location=location,
        gcp_conn_id=bigquery_conn_name,
        if_exists="ignore"
    )
    
    get_data = BigQueryGetDataOperator(
        task_id='get_data',
        project_id=project_id,
        dataset_id='sprint',  # 여기는 여러분의 bigquery 내용물을 확인하면서!
        table_id='trainer',
        location=location,
        gcp_conn_id=bigquery_conn_name,
        max_results=10
    )
    
    bigquery_hook = BigQueryHook(
        gcp_conn_id=bigquery_conn_name,
        location=location
    ).get_sqlalchemy_engine()
    
    ## python 작업 생성
    @task(task_id = 'fetch_bigquery_data')
    def fetch_bigquery_data():
        df = pd.read_sql(
            sql=f"SELECT * FROM {project_id}.sprint.battle",  # 여기에 맞게 수정하세요
            con=bigquery_hook
        )
    
        print(df.head())
        print(df.shape)
        return df

    # ✅ Task 정의 및 의존성 설정
    fetched_data = fetch_bigquery_data()
    create_dataset >> get_data >> fetched_data  # DAG 의존성 설정
