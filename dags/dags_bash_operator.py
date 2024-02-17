from airflow.models.dag import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_operator",
    schedule="0 0 * * *", # 순서대로 분, 시, 일, 월, 요일 - 매일 0시 0분에 스케줄
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"), 
    catchup=False, # 현재 날짜 - start_date 간의 누락된 시점의 구간을 돌릴 것인지
    # dagrun_timeout=datetime.timedelta(minutes=60), # timeout(실패지점) 기준 설정
    tags=["test"],
    # params={"example_key": "example_value"}, # 사용할 파라미터 설정
) as dag:
    
        # [START howto_operator_bash]
    bash_t1 = BashOperator(
        task_id="bash_t1", # airflow 상에 노출되는 task명. 일반적으로 객체명과 통일함
        bash_command="echo whoami",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2", # airflow 상에 노출되는 task명. 일반적으로 객체명과 통일함
        bash_command="echo $HOSTNAME",
    )

    bash_t1 >> bash_t2