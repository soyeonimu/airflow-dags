
import pendulum, random
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = dict(
    owner = 'soyeon6885',
    email = ['jaechanjo@airflow.com'],
    email_on_failure = False,
    retries = 3
    )

with DAG(
    dag_id="13_mysql_operator_dag",
    start_date=pendulum.datetime(2024, 11, 10, tz='Asia/Seoul'),
    schedule="30 10 * * *", # cron 표현식
    tags = ['20250224'],
    default_args = default_args,
    catchup=False
):
    create_table = SQLExecuteQueryOperator(
        task_id = "create_table",
        conn_id = "mysql_connection",
        sql = "CREATE TABLE IF NOT EXISTS temp(id INT, NAME VARCHAR(10));",
        database = 'airflow_mysql',
        autocommit=True
    )

    insert_rows = SQLExecuteQueryOperator(
        task_id = "insert_rows",
        conn_id = "mysql_connection",
        sql = "INSERT INTO temp VALUES(1,'Ryan'),(2,'Alice'),(3,'Tom');",
        database = 'airflow_mysql',
        autocommit=True
    )

    update_rows = SQLExecuteQueryOperator(
        task_id = "update_rows",
        conn_id = "mysql_connection",
        sql = "UPDATE temp SET NAME='Peter' WHERE id=3;",
        database = 'airflow_mysql',
        autocommit=True
    )

    delete_rows = SQLExecuteQueryOperator(
        task_id = "delete_rows",
        conn_id = "mysql_connection",
        sql = "DELETE FROM temp WHERE id=1",
        database = 'airflow_mysql',
        autocommit=True
    )
    
    create_table >> insert_rows >> update_rows >> delete_rows