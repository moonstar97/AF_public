from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from hooks.custom_postgres_hook import CustomPostgresHook

with DAG(
        dag_id='dags_python_with_custom_hook_bulk_load',
        start_date=pendulum.datetime(2024, 3, 30, tz='Asia/Seoul'),
        schedule='0 7 * * *',
        catchup=False
) as dag:
    def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        custom_postgres_hook = CustomPostgresHook(postgres_conn_id=postgres_conn_id)
        custom_postgres_hook.bulk_load(table_name=tbl_nm, file_name=file_nm, delimiter='\\', is_header=True, is_replace=True)

    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id': 'conn-db-postgres-custom',
                   'tbl_nm':'TbCorona19CountStatus_bulk2',
                   # 코로나 api 불러오는 dag을 unpause 해놔서 파일이 하나만 있음
                   # {{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}
                   'file_nm':'/opt/airflow/files/TbCorona19CountStatus/20240316/TbCorona19CountStatus.csv'}
    )