from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(

    # dag 생성
    dag_id='01_bash_operator',
    description='bash',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=timedelta(minutes=1)

) as dag:
    
    # 태스크 생성

    # start 말하는 태스크
    t1 = BashOperator(
        task_id='echo',
        bash_command='echo start!!'
    )

    # 여러 줄의 명령어 실행하는 태스크

    # 내가 실행하고 싶은 명령어 여러 줄 적기
    my_command = '''
        {% for i in range(5) %}
            echo {{ ds }}
            echo {{ i }}
        {% endfor %}
    '''
    # ds: 현재 날짜 정보 출력하는 내장변수

    # 태스크
    t2 = BashOperator(
        task_id='for',
        bash_command=my_command
    )

    # 태스크 연결
    t1 >> t2