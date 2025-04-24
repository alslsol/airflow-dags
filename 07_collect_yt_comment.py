from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from utils.yt_data import * # utils 폴더의 yt_data 파일에서 >> 모든 함수 가져오기

def my_task(): # 외부에 작성된 파이썬 파일의 함수를 묶은 함수
    target_handle = 'BBCNews'
    data = get_handle_to_comments(youtube, target_handle)
    save_to_hdfs(data, '/input/yt-data')

with DAG(
    dag_id='07_yt_data',
    description='yt-data',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=timedelta(minutes=10) # 10분마다 업로드
) as dga:
    t1 = PythonOperator(
        task_id='yt',
        python_callable=my_task
    )