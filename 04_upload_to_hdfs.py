from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import subprocess # 파이썬 코드 안에서 리눅스 코드 쓸 수 있는 내장 모듈

# 함수 로직: 리뷰 데이터 폴더 목록 읽고 >> hdfs에 업로드 >> 완료하면 기존 파일 삭제
def upload_to_hdfs():
    # 1) 경로 설정
    # 현재 경로
    local_dir = os.path.expanduser('~/damf2/data/review_data')
        # expanduser: home 폴더(~위치)부터 위치 지정하게 해줌
    # hdfs 경로
    hdfs_dir = '/input/review_data'

    # 2) hdfs에 폴더 생성
    # 파이썬 코드 안에서 리눅스 코드 쓰기
    subprocess.run(['hdfs', 'dfs', '-mkdir', '-p', hdfs_dir]) # 내가 쓰고 싶은 리눅스 명령어를 한 요소씩 list로 작성

    # 3) 각 리뷰 데이터 경로 >> 리스트로 묶기
    files = []

    # 리뷰 데이터 폴더 내 모든 파일 읽기
    for file in os.listdir(local_dir):
        files.append(file) # 각 파일 이름 하나씩 files 리스트에 저장
    # 4) hdfs 업로드
    for file_path in files: # 하나씩 hdfs에 올리기
        local_file_path = os.path.join(local_dir, file_path)
            # os.path.join: 앞뒤 경로 하나로 합쳐줌 >> /home/ubuntu ~ 부터 시작하는 경로로 각 파일 경로 생성됨
        hdfs_file_path = f'{hdfs_dir}/{file}'

        # hdfs에 올리기
        subprocess.run(['hdfs', 'dfs', '-put', local_file_path, hdfs_file_path])
        
        # 5) 업로드 끝난 파일 삭제
        os.remove(local_file_path)

with DAG(
    dag_id='04_upload_to_hdfs',
    description='upload',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=timedelta(minutes=5) # 5분마다 데이터 덩어리 hdfs에 업로드
) as dag:
    t1 = PythonOperator(
        task_id='upload',
        python_callable=upload_to_hdfs
    )
    t1