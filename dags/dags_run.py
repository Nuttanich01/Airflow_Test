from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago 

with DAG(
        dag_id='test_airflow_run_job',
        start_date=days_ago(0),
        schedule_interval="@daily",
        tags=["Test"]
) as dag:
    
    task1 = BashOperator(
        task_id="extract_data",
        bash_command='python /opt/airflow/dags/extract.py'
    )
    task2 = BashOperator(
        task_id="transform_data",
        bash_command='python /opt/airflow/dags/transforms.py'
    )
    task3 = BashOperator(
        task_id="load_data",
        bash_command='python /opt/airflow/dags/load.py'
    )

    task1 >> task2 >> task3 
