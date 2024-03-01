from airflow.models.dag import DAG
import datetime
import pendulum
from airflow.operators.python import PythonOperator
import random

with DAG(
    dag_id="dags_python_operator",
    schedule="30 6 * * *", # cron schedule: 순서대로 분, 시, 일, 월, 요일 - 매일 0시 0분에 스케줄
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"), 
    catchup=False, # 현재 날짜 - start_date 간의 누락된 시점의 구간을 돌릴 것인지
    # dagrun_timeout=datetime.timedelta(minutes=60), # timeout(실패지점) 기준 설정
    tags=["test"],
    # params={"example_key": "example_value"}, # 사용할 파라미터 설정
) as dag:
    def select_fruit():
        fruit = ['APPLE', 'BANANA', 'ORANGE', 'AVOCADO']
        rand_int = random.randint(0,3)
        print(fruit[rand_int])
    
    py_t1 = PythonOperator(
        task_id='py_t1',
        python_callable=select_fruit
    )

    py_t1