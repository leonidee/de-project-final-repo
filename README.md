



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

Up spark in docker containers with compose:

```shell
docker compose up --build -d spark-master spark-worker-1 spark-worker-2 spark-worker-3 spark-worker-4
```

To see logs run:

```shell
docker logs spark-master --follow 
```


Submit job:

```shell
make submit-job
```



# Else

Register kafka topic in transaction service to get messages

```shell
curl -X POST https://order-gen-service.sprint9.tgcloudenv.ru/project/register_kafka \
-H 'Content-Type: application/json; charset=utf-8' \
--data-binary @- << EOF
{
    "student": "$YP_LOGIN",
    "kafka_connect":{
        "host": "$YC_KAFKA_BROKER_HOST",
        "port": 9091,
        "topic": "$TOPIC_NAME",
        "producer_name": "$YC_KAFKA_USERNAME",
        "producer_password": "$YC_KAFKA_PASSWORD"
    }
}
EOF
```
Download certificate to work with Yandex cloud

```shell
curl "https://storage.yandexcloud.net/cloud-certs/CA.pem" -o ./CA.pem   
```


Install `kafkacat`

```shell
sudo apt update && sudo apt install kafkacat -y
```