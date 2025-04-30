import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import pandas as pd

default_args = dict(
    owner='soyeonkim',
    email=['scatterfragrance@gmail.com'],
    email_on_failure=False,
    retries=1
)

BUCKET_NAME = "sprint_bucket_soyeon" # <-- GCS의 버킷 이름
GCP_CONN_ID = "bigquery_connection" # 사실, 'bigquery_connection'와 같은 것.google_cloud_connection

with DAG(
    dag_id="20_gcs_upload_download_dag",
    start_date=pendulum.datetime(2024, 11, 10, tz="Asia/Seoul"),
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["20250224", "gcs"]
):

    @task
    def create_local_file() -> str:
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"]
        })
        path = "/opt/airflow/data/my_file.csv" # docker의 볼륨 마운트 했던 data 폴더
        df.to_csv(path, index=False)
        return path

    @task
    def upload_to_gcs(local_path: str):
        hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
        hook.upload(
            bucket_name=BUCKET_NAME,
            object_name="data/my_file.csv", # gcs 안에서도 data 폴더가 있어야 함.
            filename=local_path
        )
        print(f"Uploaded {local_path} → gs://{BUCKET_NAME}/data/my_file.csv")

    @task
    def download_from_gcs() -> str:
        hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
        destination = "/opt/airflow/data/downloaded_file.csv"
        hook.download(
            bucket_name=BUCKET_NAME,
            object_name="data/my_file.csv",
            filename=destination
        )
        print(f"Downloaded gs://{BUCKET_NAME}/data/my_file.csv → {destination}")
        return destination

    @task
    def read_file(local_path: str):
        df = pd.read_csv(local_path)
        print("Downloaded file contents:")
        print(df)
        
        return df

    file_path = create_local_file()
    upload = upload_to_gcs(file_path)
    downloaded_path = download_from_gcs()
    df = read_file(downloaded_path)

    file_path >> upload >> downloaded_path >> df # upload와 download는 함수의 인자 연결이 없기에 지정 필요! (병렬X)