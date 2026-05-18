from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'kelompok_15',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    'orders_data_pipeline',
    default_args=default_args,
    schedule_interval='*/30 * * * *',  # Berjalan setiap 30 menit
    catchup=False,
    max_active_runs=1,
    description='Pipeline: Fetch Orders API → PySpark Processing → ClickHouse',
    tags=['orders', 'kelompok15', 'mci2026']
) as dag:

    fetch_orders = BashOperator(
        task_id='fetch_orders_from_api',
        bash_command='python /opt/airflow/dags/scripts/fetch_orders.py'
    )

    process_and_load = BashOperator(
        task_id='process_and_load_to_clickhouse',
        bash_command='python /opt/airflow/dags/scripts/process_orders.py'
    )

    fetch_orders >> process_and_load
