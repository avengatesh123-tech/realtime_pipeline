from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
default_args = {
    'owner': 'vengadesan',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'de_project_monitor',
    default_args=default_args,
    description='Monitoring MySQL for new data',
    schedule_interval='* * * * *1
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

    check_data_count = MySqlOperator(
        task_id='check_mysql_count',
        mysql_conn_id='mysql_conn', 
        sql="SELECT COUNT(*) FROM orders;"
    )

 
    def print_status():

    log_health = PythonOperator(
        task_id='log_health_status',
        python_callable=print_status
    )
