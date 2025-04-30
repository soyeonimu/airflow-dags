# Airflow DAGs for Portfolio

This repository contains a collection of DAGs I created using Apache Airflow.  
Each DAG demonstrates different features such as task dependencies, Python operators, XCom, MySQL integration, and GCS file handling.

## Included DAGs

- `my_first_dag.py` – basic Hello World DAG
- `python_dag.py` – PythonOperator example
- `xcom_dags.py` – XCom usage demonstration
- `13_mysql_operator_dag.py` – MySQLOperator usage
- `15_bigquery_operator_dag.py` – BigQueryOperator with GCP
- `20_gcs_upload_download_dag.py` – GCS upload/download with BashOperator
- `air_quality_data_dag.py` – custom ETL DAG for air quality data

---

## 💡 Usage

These DAGs were tested on Airflow 2.10.5 with a local Docker-based environment.
