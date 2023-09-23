include .env
export

help:
	@echo "Usage: make [COMMAND]... [OPTIONS]..."
	@echo "\nkafkacat commands:"
	@echo "    make run-producer topic=<topic_name> 		Run kafkacat in producer mode"
	@echo "    make run-consumer topic=<topic_name> 		Run kafkacat in consumer mode"
	@echo "\nCommands for Spark:"
	@echo "    make run-job job=<file_name>			Run one of the Spark jobs listed in ./jobs folder"


# kafkacat commands
run-producer:
	kcat -b $$YC_KAFKA_BROKER_HOST:$$YC_KAFKA_BROKER_PORT \
		-X security.protocol=SASL_SSL \
		-X sasl.mechanisms=SCRAM-SHA-512 \
		-X sasl.username=$$YC_KAFKA_USERNAME \
		-X sasl.password=$$YC_KAFKA_PASSWORD \
		-X ssl.ca.location=./CA.pem \
		-P -t $(topic)

run-consumer:
	kcat -b $$YC_KAFKA_BROKER_HOST:$$YC_KAFKA_BROKER_PORT \
		-X security.protocol=SASL_SSL \
		-X sasl.mechanisms=SCRAM-SHA-512 \
		-X sasl.username=$$YC_KAFKA_USERNAME \
		-X sasl.password=$$YC_KAFKA_PASSWORD \
		-X ssl.ca.location=./CA.pem \
		-C -o $(offset) -t $(topic) \
		-f 'Key: %k\nValue: %s\nPartition: %p\nOffset: %o\nTimestamp: %T\n'

