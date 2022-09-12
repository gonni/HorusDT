from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'horus-air',
    'retries': 0,
    'retry_delay': timedelta(seconds=20),
    'depends_on_past': False
}

dag_horus_lda = DAG(
    'bash-spark-horus-lda',
    start_date=days_ago(0),
    default_args=default_args,
    schedule_interval='30 * * * *',
    catchup=False,
    is_paused_upon_creation=False,
)

cmd="/Users/horus/dev/docker-runtime/spark-build/cmd/run_job_lda_1h.sh "

#시작을 알리는 dummy
task_start = DummyOperator(
    task_id='start_horus_lda',
    dag=dag_horus_lda,
)
run_task = BashOperator(
    task_id='spark_run_horus_lda',
    dag=dag_horus_lda,
    bash_command=cmd,
)

#의존관계 구성
task_start >> run_task