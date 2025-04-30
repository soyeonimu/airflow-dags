import os
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from io import BytesIO, StringIO

# 환경변수 설정하기 (docker 컨테이너 경로 기준)
load_dotenv("/opt/airflow/data/.env")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/data/proven-octane-456123-s1-003d59e985db.json"

# 1. API 호출 함수
def call_api(**kwargs):
    key = os.getenv('SERVICE_KEY')
    url = 'http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty'
    params = {
        'serviceKey': key,
        'returnType': 'xml',
        'numOfRows': '10000',
        'sidoName': '전국',
        'ver': '1.0'
    }
    response = requests.get(url, params=params)
    if response.status_code == 200 and response.text:
        kwargs['ti'].xcom_push(key='api_data', value=response.text)
    else:
        raise Exception(f"API 호출 실패: {response.status_code} - {response.text}")

# 2. 태그 값 추출 함수
def convert_string(item_, key_):
    try:
        return item_.find(key_.lower()).text.strip()
    except AttributeError:
        return None

# 3. XML -> DataFrame
def parse_to_dataframe(**kwargs):
    ti = kwargs['ti']
    xml_data = ti.xcom_pull(task_ids='call_api', key='api_data')
    if not xml_data:
        raise ValueError("API 데이터가 없습니다.")

    xml = BeautifulSoup(xml_data, "lxml")
    items = xml.find_all("item")
    item_list = []

    for item in items:
        item_dict = {
            '측정소명': convert_string(item, "stationName"),
            '측정망 정보': convert_string(item, "mangName"),
            '시도명': convert_string(item, "sidoName"),
            '측정일시': convert_string(item, "dataTime"),
            '아황산가스 농도': convert_string(item, "so2Value"),
            '일산화탄소 농도': convert_string(item, "coValue"),
            '오존 농도': convert_string(item, "o3Value"),
            '이산화질소 농도': convert_string(item, "no2Value"),
            '미세먼지 PM10 농도': convert_string(item, "pm10Value"),
            '미세먼지 PM10 24시간 예측이동농도': convert_string(item, "pm10Value24"),
            '초미세먼지 PM2point5 농도': convert_string(item, "pm25Value"),
            '초미세먼지 PM2point5 24시간 예측이동농도': convert_string(item, "pm25Value24"),
            '통합대기환경수치': convert_string(item, "khaiValue"),
            '통합대기환경지수': convert_string(item, "khaiGrade"),
            '아황산가스 지수': convert_string(item, "so2Grade"),
            '일산화탄소 지수': convert_string(item, "coGrade"),
            '오존 지수': convert_string(item, "o3Grade"),
            '이산화질소 지수': convert_string(item, "no2Grade"),
            '미세먼지 PM10 24시간 등급': convert_string(item, "pm10Grade"),
            '초미세먼지 PM2point5 24시간 등급': convert_string(item, "pm25Grade"),
            '미세먼지 PM10 1시간 등급': convert_string(item, "pm10Grade1h"),
            '초미세먼지 PM2point5 1시간 등급': convert_string(item, "pm25Grade1h"),
            '아황산가스 플래그': convert_string(item, "so2Flag"),
            '일산화탄소 플래그': convert_string(item, "coFlag"),
            '오존 플래그': convert_string(item, "o3Flag"),
            '이산화질소 플래그': convert_string(item, "no2Flag"),
            '미세먼지 PM10 플래그': convert_string(item, "pm10Flag"),
            '초미세먼지 PM2point5 플래그': convert_string(item, "pm25Flag"),
        }
        item_list.append(item_dict)

    df = pd.DataFrame(item_list)

    # 📌 Unix timestamp 형태라면 변환, 그 외는 그대로
    df['측정일시'] = pd.to_datetime(df['측정일시'], errors='coerce')
    df['측정일시'] = df['측정일시'].dt.strftime('%Y-%m-%d %H:%M:%S')
    ti.xcom_push(key='processed_data', value=df.to_json(orient='split'))


# 4. BigQuery 적재 함수
def load_to_bigquery(**kwargs):
    ti = kwargs['ti']
    df_json = ti.xcom_pull(task_ids='parse_to_dataframe', key='processed_data')
    if not df_json:
        raise ValueError("DataFrame 데이터가 존재하지 않습니다.")

    df = pd.read_json(StringIO(df_json), orient='split')

    client = bigquery.Client()
    dataset_id = "mission18"
    table_id = "air_quality_total"
    table_ref = client.dataset(dataset_id).table(table_id)

    job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,  # 헤더 한 줄은 건너뛰기
    autodetect=True
)


    column_order = [
        '측정소명', '측정망 정보', '시도명', '측정일시',
        '아황산가스 농도', '일산화탄소 농도', '오존 농도', '이산화질소 농도',
        '미세먼지 PM10 농도', '미세먼지 PM10 24시간 예측이동농도',
        '초미세먼지 PM2point5 농도', '초미세먼지 PM2point5 24시간 예측이동농도',
        '통합대기환경수치', '통합대기환경지수', '아황산가스 지수', '일산화탄소 지수',
        '오존 지수', '이산화질소 지수',
        '미세먼지 PM10 24시간 등급', '초미세먼지 PM2point5 24시간 등급',
        '미세먼지 PM10 1시간 등급', '초미세먼지 PM2point5 1시간 등급',
        '아황산가스 플래그', '일산화탄소 플래그', '오존 플래그', '이산화질소 플래그',
        '미세먼지 PM10 플래그', '초미세먼지 PM2point5 플래그'
    ]

    df = df[column_order]

    with BytesIO() as csv_data:
        df.to_csv(csv_data, index=False, encoding='utf-8', header=True)
        csv_data.seek(0)
        job = client.load_table_from_file(csv_data, table_ref, job_config=job_config)
        job.result()

# 5. DAG 정의

default_args = {
    'owner': 'soyeonkim',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 8),
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id="air_quality_data_dag",
    start_date=pendulum.datetime(2024, 4, 8, tz='Asia/Seoul'),
    schedule_interval='@hourly',
    tags=['mission18'],
    default_args=default_args,
    catchup=False
)

api_task = PythonOperator(
    task_id='call_api',
    python_callable=call_api,
    provide_context=True,
    dag=dag
)

parse_task = PythonOperator(
    task_id='parse_to_dataframe',
    python_callable=parse_to_dataframe,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_bigquery',
    python_callable=load_to_bigquery,
    provide_context=True,
    dag=dag
)

api_task >> parse_task >> load_task