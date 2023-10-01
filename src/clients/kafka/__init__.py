import time
from os import getenv

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from src import get_logger

log = get_logger(name=__name__)

DEFAULT_DELAY = 2  # Delay in sec between attemts to connect

YC_KAFKA_BROKER_HOST = getenv("YC_KAFKA_BROKER_HOST")
YC_KAFKA_BROKER_PORT = getenv("YC_KAFKA_BROKER_PORT")
YC_KAFKA_USERNAME = getenv("YC_KAFKA_USERNAME")
YC_KAFKA_PASSWORD = getenv("YC_KAFKA_PASSWORD")
CERTIFICATE_PATH = getenv("CERTIFICATE_PATH")

if not all(
    [
        YC_KAFKA_BROKER_HOST,
        YC_KAFKA_BROKER_PORT,
        YC_KAFKA_USERNAME,
        YC_KAFKA_PASSWORD,
        CERTIFICATE_PATH,
    ]
):
    raise ValueError(
        "Required environment varialbes not set. See .env.template for more details"
    )

properties = dict(
    bootstrap_servers=f"{YC_KAFKA_BROKER_HOST}:{YC_KAFKA_BROKER_PORT}",
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_plain_username=YC_KAFKA_USERNAME,
    sasl_plain_password=YC_KAFKA_PASSWORD,
    ssl_cafile=CERTIFICATE_PATH,
)


def get_producer() -> KafkaProducer:
    log.info("Connecting to Kafka cluster in producer mode")

    log.debug(f"Delay between retries set to {DEFAULT_DELAY}")

    for i in range(1, 10 + 1):
        try:
            producer = KafkaProducer(**properties)

            assert producer.bootstrap_connected(), "Not connected!"
            return producer

        except NoBrokersAvailable as err:
            if i == 10:
                raise err
            else:
                log.warning(f"{err}. Retrying...")
                time.sleep(DEFAULT_DELAY)

                continue
