from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


# pip install boto3 python-dotenv
from dotenv import load_dotenv
import boto3

import os

# 환경변수 설정
load_dotenv('/home/ubuntu/airflow/.env')

def upload_to_s3():

    # s3 접근하기
    s3 = boto3.client(
        's3', # s3 연결할 것이라 지정
        aws_access_key_id=os.getenv('AWS_KEY'),
        aws_secret_access_key=os.getenv('AWS_SECRET'),
        region_name='ap-northeast-2' # 서울 region에 버킷 만들었기에 이거 적어주기
    )

    # 로컬/s3 파일 경로 지정
    local_dir = os.path.expanduser('~/damf2/data/bitcoin')
    bucket_name = 'damf2-och' # 저장소 자체 이름
    s3_prefix = 'bitcoin-sori/' # 버킷 내 폴더
    
    files = []

    for file in os.listdir(local_dir):
        files.append(file)

    for file in files:
        local_file_path = os.path.join(local_dir, file)
        s3_path = f'{s3_prefix}{file}'

        # s3에 업로드
        s3.upload_file(local_file_path, bucket_name, s3_path)
            # <현재 파일>, <저장될 버킷 이름>, <해당 버킷 경로>
        os.remove(local_file_path)

with DAG(
    dag_id='06_upload',
    description='upload',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=timedelta(minutes=5)
) as dag:
    t1 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3
    )