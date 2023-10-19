

# Jobs

## transaction-service-stream-collector

Loading envvitonment variables into active shell session:

```shell
source .env
```

Submit application in `DEV` mode:

```shell
docker exec -it spark-master \
	bash -c \
	"cd /app \
	&& source /app/.venv/bin/activate \
	&& /opt/bitnami/spark/bin/spark-submit \
	--verbose \
	--deploy-mode client \
	--master $SPARK_MASTER_URL \
	--driver-cores 2 \
	--driver-memory 4G \
	--executor-cores 1 \
	--executor-memory 2G \
	--conf spark.executor.instances=8 \
	--conf spark.dynamicAllocation.enabled=true \
	--conf spark.dynamicAllocation.maxExecutors=8 \
	--conf spark.sql.shuffle.partitions=8 \
	--conf spark.pyspark.virtualenv.enabled=true \
	--conf spark.pyspark.virtualenv.type=native \
	--conf spark.pyspark.python=/app/.venv/bin/python3.11 \
	--conf spark.pyspark.virtualenv.bin.path=/app/.venv/bin \
	--conf spark.ui.enabled=false \
	--conf spark.sql.adaptive.enabled=false \
	--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
	/app/src/transaction_service_stream_collector/runner.py \
	--mode=DEV \
	--spark-log-level=WARN"
```


# Services

## Yandex cloud

I personally use `yc` utility.

### Kafka

```shell
source .env
```

Start created cluster

```shell
yc managed-kafka cluster start $YC_KAFKA_SERVICE_ID --async
```

Create topic 

```shell
export TOPIC_NAME=transaction-service-input
```

```shell
yc managed-kafka topic create $TOPIC_NAME \
    --cluster-id $YC_KAFKA_SERVICE_ID \
    --partitions 3 \
    --replication-factor 2 \
    --compression-type gzip \
    --cleanup-policy delete \
    --async
```

And new user with permitions on it

```shell
yc managed-kafka user create $YC_KAFKA_USERNAME \
    --cluster-id $YC_KAFKA_SERVICE_ID \
    --password $YC_KAFKA_PASSWORD \
    --permission topic=$TOPIC_NAME,role=producer,role=consumer \
    --async
```


## Docker compose 


### Spark

```shell
export APP_NAME=spark \
	&& export APP_RELEASE=v$(date +"%Y%m%d")-r1.0
```

```shell
docker build -f docker/spark/Dockerfile -t $APP_NAME:$APP_RELEASE .
```

Set environment variable to indicate docker compose which images to use:

```shell
export SPARK_IMAGE=$APP_NAME:$APP_RELEASE
```

Up spark in docker containers with compose:

```shell
docker compose up -d
```

Now check that Spark cluster health and running:

```shell
docker logs spark-master --follow 
```




# Else


Download certificate to work with Yandex cloud

```shell
curl "https://storage.yandexcloud.net/cloud-certs/CA.pem" -o ./CA.pem   
```


Install `kafkacat`

```shell
sudo apt update && sudo apt install kafkacat -y
```

```shell
make run-consumer topic=transaction-service-input offset=begining
```




# Producer 

```shell
export APP_NAME=transaction-service-input-producer \
	&& export APP_RELEASE=v$(date +"%Y%m%d")-r1.0
```

```shell
export PRODUCER_IMAGE=$APP_NAME:$APP_RELEASE
```

```shell
docker build -f docker/producer/Dockerfile -t $PRODUCER_IMAGE .
```

```shell
docker run -it --rm --name transaction-service-input-producer $PRODUCER_IMAGE
```


