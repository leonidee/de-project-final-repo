from __future__ import annotations

from datetime import datetime
from os import getenv

import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T
import yaml
from pyspark.sql.utils import AnalysisException
from pyspark.storagelevel import StorageLevel

from src.logger import get_logger

log = get_logger(__name__)

TODAY = datetime.today()

with open(f'{getenv("APP_PATH")}/config.yaml') as f:
    config = yaml.safe_load(f)["stream"]


def get_query(
    spark: pyspark.sql.SparkSession, mode: str
) -> pyspark.sql.streaming.DataStreamWriter:
    frame: pyspark.sql.DataFrame = _read_stream(spark=spark, config=config)

    match mode:
        case "dev":
            return (
                frame.writeStream.format("console")
                .outputMode("append")
                .queryName(config["query-name"])
                .trigger(processingTime=config["processing-time"])
                .options(
                    truncate=False,
                    checkpointLocation=f'{config["checkpoint-location"]}/{config["app-name"]}/{config["query-name"]}',
                )
            )
        case "prod":
            return (
                frame.writeStream.queryName(config["query-name"])
                .trigger(processingTime=config["processing-time"])
                .foreachBatch(func=_foreach_batch_func)
                .options(
                    checkpointLocation=f'{config["checkpoint-location"]}/{config["app-name"]}/{config["query-name"]}',
                )
            )


def _read_stream(
    spark: pyspark.sql.SparkSession, config: dict
) -> pyspark.sql.DataFrame:
    BOOTSTRAP_SERVER = (
        f"{getenv('YC_KAFKA_BROKER_HOST')}:{getenv('YC_KAFKA_BROKER_PORT')}"
    )
    USERNAME = getenv("YC_KAFKA_USERNAME")
    PASSWORD = getenv("YC_KAFKA_PASSWORD")
    CERTIFICATE_PATH = f'{getenv("APP_PATH")}/CA.pem'

    if not all([BOOTSTRAP_SERVER, USERNAME, PASSWORD]):
        raise ValueError(
            "Required environment varialbes not set. See .env.template for more details"
        )

    log.info(f"Subscribe to {config['topic']} kafka topic")

    options = {
        "kafka.bootstrap.servers": BOOTSTRAP_SERVER,
        "startingOffsets": "earliest",
        "subscribe": config["topic"],
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "SCRAM-SHA-512",
        "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{USERNAME}" password="{PASSWORD}";',
        "kafka.ssl.truststore.type": "PEM",
        "kafka.ssl.truststore.location": CERTIFICATE_PATH,
        "maxOffsetsPerTrigger": "100",
    }

    df = (
        spark.readStream.format("kafka")
        .options(**options)
        .load()
        .select(
            F.col("value").cast(T.StringType()),
        )
        .withColumn(
            "value",
            F.from_json(
                col=F.col("value"),
                schema=T.StructType(
                    [
                        T.StructField("object_id", T.StringType(), True),
                        T.StructField("object_type", T.StringType(), True),
                        T.StructField("sent_dttm", T.StringType(), True),
                        T.StructField("payload", T.StringType(), True),
                    ]
                ),
            ),
        )
        .select(
            F.col("value.object_id").alias("object_id"),
            F.col("value.object_type").alias("object_type"),
            F.col("value.sent_dttm").alias("sent_dttm"),
            F.col("value.payload").alias("payload"),
        )
        .withColumn(
            "sent_dttm",
            F.to_timestamp(
                F.regexp_replace(F.col("sent_dttm"), "T", " "),
                r"yyyy-MM-dd HH:mm:ss",
            ),
        )
        .withColumn("object_type", F.lower(F.col("object_type")))
    )

    return df


def _write_dataframe(frame: pyspark.sql.DataFrame, path: str) -> ...:
    log.info(f"Wriring dataframe to {path} path")

    try:
        frame.write.parquet(
            path=path,
            mode="errorifexists",
        )
        log.info(f"Done! Results -> {path}")

    except AnalysisException as err:
        log.warning(f"Notice that {str(err)}. Overwriting")

        frame.write.parquet(
            path=path,
            mode="overwrite",
        )
        log.info(f"Done! Results -> {path}")


def _foreach_batch_func(frame: pyspark.sql.DataFrame, batch_id: int) -> ...:
    frame.persist(StorageLevel.MEMORY_ONLY)

    _write_dataframe(
        frame=frame.where(F.col("object_type") == "currency"),
        path=f"{config['output-path']}/datekey={TODAY.strftime(r'%Y%m%d')}/object_type=currency/batch_id={batch_id}",
    )

    _write_dataframe(
        frame=frame.where(F.col("object_type") == "transaction"),
        path=f"{config['output-path']}/datekey={TODAY.strftime(r'%Y%m%d')}/object_type=transaction/batch_id={batch_id}",
    )

    frame.unpersist()
