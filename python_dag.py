from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# 파이썬 함수 정의
def hello_name(name, **kwargs):
    print(f"Hello {name}!")
    print(kwargs['templates_dict']['date']) # Jinja 템플릿은 templates_dict 키 라는 서랍장에 넣었다가 뺐다가 하면서 쓰는 것!

# DAG 정의
with DAG(
        "example-dag-python",
        description="PythonOperator 예제",
        schedule=timedelta(days=1),
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=["example"],
) as dag:

    say_hello = PythonOperator(
        task_id="python1",
        python_callable=hello_name,
        op_kwargs={"name": 22},
        templates_dict={"date": "{{ ds }}"}
    )
