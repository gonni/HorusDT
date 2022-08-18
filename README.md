# HorusDT

### run on docker env
scp HorusDT-assembly-0.1.0-SNAPSHOT3.jar jeff@192.168.35.4:/home/jeff/dev/temp

scp target/scala-2.12/HorusDT-assembly-0.1.0-SNAPSHOT.jar jeff@192.168.35.4:/home/jeff/dev/docker-airflow-spark/airflow-spark/spark/app

docker exec -it docker_spark_1 spark-submit --master spark://spark:7077 <spark_app_path> [optional]<list_of_app_args>

docker exec -it docker_spark_1 spark-submit --master spark://spark:7077 /usr/local/spark/app/HorusDT-assembly-0.1.0-SNAPSHOT2.jar

### run on office mini server

scp target/scala-2.12/HorusDT-assembly-0.1.0-SNAPSHOT.jar yg@10.202.66.123:/home/yg/dev/docker-rt/airflow-spark/spark/resources/horus

docker exec -it docker_spark_1 spark-submit --master spark://spark:7077 <spark_app_path> [optional]<list_of_app_args>

sudo docker exec -it docker_spark_1 spark-submit \
--class com.yg.horus.dt.termcount.CrawledProcessing \
--master spark://spark:7077 \
/usr/local/spark/resources/horus/HorusDT-assembly-0.1.0-SNAPSHOT.jar



