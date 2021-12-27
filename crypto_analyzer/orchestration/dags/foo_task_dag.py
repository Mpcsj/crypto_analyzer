import os
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from crypto_analyzer.tasks.foo_distributed_data_processing.foo_task1 import FooTask1
from crypto_analyzer.tasks.foo_distributed_data_processing.foo_task2 import FooTask2
from crypto_analyzer.tasks.foo_distributed_data_processing.foo_task3 import FooTask3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [os.environ.get('ADMIN_EMAIL')],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
        dag_id='foo_task_dag',
        default_args=default_args,
        start_date=datetime(2021, 12, 26),
        schedule_interval="*/10 * * * *",  # every 10 minutes
        tags=["task_w_multiworker_stag6"]
) as dag:
    task1 = PythonOperator(
        task_id=FooTask1.name,
        python_callable=lambda: FooTask1().run()
    )
    task3 = PythonOperator(
        task_id=FooTask3.name,
        python_callable=lambda: FooTask3().run()
    )
    task2_ids = [f'{FooTask2.name}_{worker_id}' for worker_id in range(10)]
    task2_list = []
    for worker_id in range(10):
        task2_list.append(PythonOperator(
            task_id=f'{FooTask2.name}_{worker_id}',
            python_callable=lambda: FooTask2(worker_id).run()
        ))
    task1 >> task2_list >> task3
    # branching >> task2 >> join_workers>> task3
    # task1 >> task2 >> task3
