# bulk_load의 문제
# 1. load 가능한 delimiter는 tab(\)으로 고정되어 있음
# 2. header까지 포함해서 업로드됨
# 3. 특수문자로 인해 파싱이 안될 경우 오류 발생

# 개선 방안
# custom hook 생성
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
        dag_id='dags_python_with_postgres_hook_bulk_load',
        start_date=pendulum.datetime(2023, 3, 16, tz='Asia/Seoul'),
        schedule='0 7 * * *',
        catchup=False
) as dag:
    def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        postgres_hook = PostgresHook(postgres_conn_id)
        postgres_hook.bulk_load(tbl_nm, file_nm)

    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id': 'conn-db-postgres-custom',
                   'tbl_nm':'TbCorona19CountStatus_bulk1',
                   # 코로나 api 불러오는 dag을 unpause 해놔서 파일이 하나만 있음
                   # {{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}
                   'file_nm':'/opt/airflow/files/TbCorona19CountStatus/20240316/TbCorona19CountStatus.csv'}
    )