from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Basic settings
default_args = {
    'owner': 'vengadesan',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'de_project_monitor',
    default_args=default_args,
    description='Monitoring MySQL for new data',
    schedule_interval='* * * * *', # Ovvoru minute-um run aagum
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

    # Task 1: MySQL-la data count check panradhu
    check_data_count = MySqlOperator(
        task_id='check_mysql_count',
        mysql_conn_id='mysql_conn', # UI-la create panna name
        sql="SELECT COUNT(*) FROM orders;"
    )

    # Task 2: Status-ah log panradhu
    def print_status():
        print("✅ Pipeline Health: Checked MySQL table 'orders' at", datetime.now())

    log_health = PythonOperator(
        task_id='log_health_status',
        python_callable=print_status
    )

    # Order: First count check pannu, appram log pannu
    check_data_count >> log_health