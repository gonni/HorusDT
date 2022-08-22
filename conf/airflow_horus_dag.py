from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
#postgres_driver_jar = "/usr/local/spark/resources/horus/HorusDT-assembly-0.1.0-SNAPSHOT.jar"

#movies_file = "/usr/local/spark/resources/data/movies.csv"
#ratings_file = "/usr/local/spark/resources/data/ratings.csv"
#postgres_db = "jdbc:postgresql://postgres/test"
#postgres_user = "test"
#postgres_pwd = "postgres"

###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 8, 17),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    dag_id="spark-horus-sample-per20",
    description="spark ml processing",
    default_args=default_args,
    schedule_interval='*/20 * * * *'
)

start = DummyOperator(task_id="start", dag=dag)

spark_config = {
    'conf': {
        "spark.master":spark_master
    },
    'conn_id': 'spark_default',
    'application': '/usr/local/spark/resources/horus/HorusDT-assembly-0.1.0-SNAPSHOT.jar',
    'java_class': 'com.yg.hello.Main'
}

operator = SparkSubmitOperator(task_id='spark_hell_submit_task_per20m', dag=dag, **spark_config)

end = DummyOperator(task_id="end", dag=dag)

start >> operator >> end