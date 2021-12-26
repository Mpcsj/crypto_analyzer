from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from crypto_analyzer.tasks.coin_market_cap.cmc_loader import CmcLoader
from crypto_analyzer.tasks.coin_market_cap.cmc_parsers import CmcParser
from crypto_analyzer.tasks.coin_market_cap.cmc_spiders import CmcSpider

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [os.environ.get('ADMIN_EMAIL')],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        'attempt1_cmc_dag',
        default_args=default_args,
        schedule_interval=timedelta(minutes=5),
        start_date=datetime(2021, 12, 25),
        catchup=False,
        tags=['mpcsj_cmc']
) as dag:
    t0 = BashOperator(
        task_id='verify_curr_folder',
        bash_command='ls -a'
    )
    t1 = PythonOperator(
        dag=dag,
        task_id=CmcSpider.name,
        python_callable=CmcSpider.run_task
    )
    t2 = PythonOperator(
        dag=dag,
        task_id=CmcParser.name,
        python_callable=CmcParser.run_task
    )
    t3 = PythonOperator(
        dag=dag,
        task_id=CmcLoader.name,
        python_callable=CmcLoader.run_task
    )
t0 >> t1 >> t2 >> t3