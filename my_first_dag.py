from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# DAG 정의하기
with DAG(
    dag_id='my_first_dag',
    description='간단한 DAG 예제',
    schedule_interval='@daily',  # Airflow 2.0 이상에서는 schedule_interval 사용
    start_date=datetime(2025, 4, 6),
    catchup=True,
    tags=['B']
) as dag:
    
    task1 = BashOperator(
        task_id='print_date',
        bash_command='date'
    )

    task2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3
    )

    task3 = BashOperator(
        task_id='print_date2',
        bash_command='date'
    )

    # Task 순서 정의하기
    task1 >> task2 >> task3
