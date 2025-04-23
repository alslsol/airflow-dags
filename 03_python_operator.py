from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# 실행할 파이썬 함수 선언
def hello():
    print('hello world')

def bye():
    print('bye...')


with DAG(
    dag_id='02_python',
    description='python test',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=timedelta(minutes=1)
) as dag:


    t1 = PythonOperator(
        task_id='hello',
        python_callable=hello
            # 파이썬으로 실행할 수 있는 함수 이름 적음 -> hello라는 함수 실행하는 태스크
    )
    t2 = PythonOperator(
        task_id='bye',
        python_callable=bye # bye 함수 실행 태스크
    )

    t1 >> t2