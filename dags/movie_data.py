from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import (
        PythonOperator,
        PythonVirtualenvOperator,
        BranchPythonOperator
)

with DAG(
    'pyspark_movie',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='movie_summary',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2023,1,7),
    catchup=True,
    tags=['pyspark','movie'],
) as dag:


    start=EmptyOperator(task_id='start')
    end=EmptyOperator(task_id='end')
    
    re_partition = PythonVirtualenvOperator(
            task_id='re.partition',
            python_callable=fun_re_partition,
            requirements=["git+git@github.com:sooj1n/pyspark_airflow.git@main"],
            system_site_packages=False,
            #op_args=["{{ds_nodash}}"]
    )


    start >> end
    

