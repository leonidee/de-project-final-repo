from datetime import date, datetime
from os import getenv

import dotenv
import pyspark  # type: ignore
import pyspark.sql.functions as F  # type: ignore
import pyspark.sql.types as T  # type: ignore

from src.logger import get_logger

log = get_logger(__name__)
dotenv.load_dotenv()


def read_stream(spark: pyspark.sql.SparkSession, topic: str) -> pyspark.sql.DataFrame:
    BOOTSTRAP_SERVER = (
        f"{getenv('YC_KAFKA_BROKER_HOST')}:{getenv('YC_KAFKA_BROKER_PORT')}"
    )
    USERNAME = getenv("YC_KAFKA_USERNAME")
    PASSWORD = getenv("YC_KAFKA_PASSWORD")

    if not all([BOOTSTRAP_SERVER, USERNAME, PASSWORD]):
        raise ValueError("Required environment varialbes not set. See .env.template")

    options = {
        "kafka.bootstrap.servers": BOOTSTRAP_SERVER,
        "startingOffsets": "earliest",
        "subscribe": topic,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "SCRAM-SHA-512",
        "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{USERNAME}" password="{PASSWORD}";',
        "kafka.ssl.truststore.type": "PEM",
        "kafka.ssl.truststore.location": "/app/CA.pem",
    }

    df = (
        spark.readStream.format("kafka")
        .options(**options)
        .load()
        .select(
            F.col("key").cast(T.StringType()),
            F.col("value").cast(T.StringType()),
        )
    )

    return df


def get_query(
    spark: pyspark.sql.SparkSession, topic: str
) -> pyspark.sql.streaming.DataStreamWriter:
    stream = read_stream(spark, topic)

    return (
        stream.writeStream.format("console")
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .option("truncate", False)
        .start()
    )
