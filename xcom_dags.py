from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
        "example-dag-xcom",
        description="XComs 예제",
        schedule=timedelta(days=1),
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=["example"],
) as dag:

    # XCom push 예제
    def xcom_push(**context):
        xcom_value = "송신하는 데이터"
        context['task_instance'].xcom_push(key='xcom_key', value=xcom_value)

    # XCom pull 예제
    def xcom_pull(**context):
        xcom_value = context["task_instance"].xcom_pull(key='xcom_key')
        print(f"xcom value received: {xcom_value}")

    xcom_push_task = PythonOperator(
        task_id="xcom_push",
        python_callable=xcom_push
    )

    xcom_pull_task = PythonOperator(
        task_id="xcom_pull",
        python_callable=xcom_pull
    )

    xcom_push_task >> xcom_pull_task