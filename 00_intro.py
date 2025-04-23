from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='00_intro', # dag의 이름 지정 -> 필수 인자
    description='first DAG', # dag 대한 설명
    start_date=datetime(2025, 1, 1), # 언제부터 해당 작업 시작할지 >> 2025.01.01부터 작업하겠다
    catchup=False, # 내가 4월 23일에 작업 시작했다면, 초기 시작 설정과의 빈 기간 동안의 작업 할 것인지 >> 안 하고 지금부터 할거란 것
    schedule=timedelta(minutes=1), # 얼마 간격으로 작업 할 지 >> 매분 시작
    # schedule='* * * * *' # 1분에 한번씩 시작해 달란 것
) as dag: # as dag: DAG 인스턴스한 것과 동일
    
    # 태스크 만들기
    t1 = BashOperator(
        task_id='first_task',
        bash_command='date' # 날짜/시간 찍는 일하는 태스크
    )

    t2 = BashOperator(
        task_id='second_task',
        bash_command='echo hello!!' # hello!!라고 출력하는 태스크
    )

    # 태스크 연결
    t1 >> t2 # t1 작업 진행 후 t2 진행