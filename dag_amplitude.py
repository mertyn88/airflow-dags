from airflow import DAG

# Operator
from airflow.operators.python_operator import PythonOperator
# Util
from datetime import datetime, timedelta
import pendulum
# Custom
from util.slack_alert import SlackAlert
from amplitude.workflow import job_download
from amplitude.workflow import job_classification
from amplitude.workflow import job_remove
from amplitude.workflow import job_upload
from amplitude.workflow import job_hdfs
import logging

# Init logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Format logger
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d] [%(funcName)s] [%(message)s]'))
logger.addHandler(stream_handler)

default_args = {
    'owner': 'airflow'
    , 'catchup': False                                  # 정해진 시간에 실행되지 못한 DAG를 늦게라도 실행 처리
    , 'execution_timeout': timedelta(hours=1)           # 설정한 시간을 초과하여 실행될 경우, 실패로 처리
    , 'depends_on_past': False                          # 이전 Dag run에서 현재 task에 대해 실패시 이후 처리
    # , 'concurrency': 5                                # 최대 task 실행 개수
    # , 'max_active_runs': 2                            # 동시 dag 실행 개수
    , 'start_date': datetime(2022, 6, 14, 2, 0, 0, tzinfo=pendulum.timezone("Asia/Seoul"))
    , 'retries': 0
    , 'retry_delay': timedelta(minutes=0)
    , 'on_failure_callback': SlackAlert('#part-search-data').on_failure
}

with DAG(
        'amplitude_data_daily'
        , default_args=default_args
        , schedule_interval='0 2 * * *' # every day 02:00
) as dag:
    task_start = PythonOperator(
        task_id='task_start'
        , python_callable=job_remove.run
    )
    task_download = PythonOperator(
        task_id='task_download'
        , python_callable=job_download.run
    )
    task_classification = PythonOperator(
        task_id='task_classification'
        , python_callable=job_classification.run
    )
    task_upload = PythonOperator(
        task_id='task_upload'
        , python_callable=job_upload.run
    )
    task_hdfs = PythonOperator(
        task_id='task_hdfs'
        , python_callable=job_hdfs.run
    )
    task_end = PythonOperator(
        task_id='task_end'
        , python_callable=job_remove.run
    )

    task_start >> task_download >> task_classification >> task_upload >> task_hdfs >> task_end
