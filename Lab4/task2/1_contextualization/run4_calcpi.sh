sudo docker run -d -h sparkmaster myspark/first:v0 
DOCKER_ID=$(sudo docker ps --format 'json' | jq -r '.ID')
sudo docker exec -it $DOCKER_ID /bin/bash -c '$SPARK_HOME/bin/spark-submit --class org.apache.spark.examples.SparkPi --master spark://sparkmaster:7077 $SPARK_HOME/examples/jars/spark-examples_2.12-3.3.2.jar'
sudo docker stop $DOCKER_ID
