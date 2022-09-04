from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
spark_master = "spark://horus-mlui-Mac-Studio.local"
driver_class_path = "/Users/horus/dev/spark-standalone/data/mysql-connector-java-5.1.44.jar"
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
    "start_date": datetime(2022, 9, 4),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    dag_id="spark-horus-lda-1h",
    description="spark ml topic classification processing",
    default_args=default_args,
    schedule_interval='30 * * * *'
)

start = DummyOperator(task_id="start", dag=dag)

spark_config = {
    'conf': {
        "spark.master":spark_master
    },
    'conn_id': 'spark_default1',
    'num_executors': 1,
    'driver_class_path': driver_class_path,
    'application': '/Users/horus/dev/spark-standalone/data/HorusDT-assembly-0.1.5-SNAPSHOT.jar',
    'java_class': 'com.yg.horus.dt.topic.LdaJobMain',
    'application_args': ['LDA_TOPIC_21', 'spark://horus-mlui-Mac-Studio.local', '21', '60', '10', '10']
}

operator = SparkSubmitOperator(task_id='spark-horus-lda-write-per1h', dag=dag, **spark_config)

end = DummyOperator(task_id="end", dag=dag)

start >> operator >> end