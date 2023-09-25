from __future__ import annotations

from datetime import datetime
from os import getenv

import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.utils import AnalysisException
from pyspark.storagelevel import StorageLevel

from src import get_logger, parse_config

log = get_logger(__name__)

TODAY = datetime.now()

__all__ = ["get_query"]


def get_query(
    spark: pyspark.sql.SparkSession, mode: str
) -> pyspark.sql.streaming.DataStreamWriter:
    """Get transaction-service-stream-collector Spark `DataStreamWriter`.

    Consume data from given in `config.yaml` kafka topic.

    ## Parameters
    `spark` : `pyspark.sql.SparkSession`
        Active Spark Session.
    `mode` : `str`
        In which mode to submit.

    ## Returns
    `pyspark.sql.streaming.DataStreamWriter`
        DataStreamWrite object with query results.
    """
    log.info(f"Getting streaming query with {mode=}")

    config = parse_config(app="transaction-service-stream-collector", mode=mode)

    frame: pyspark.sql.DataFrame = _read_stream(spark=spark, config=config)

    def _foreach_batch_func(frame: pyspark.sql.DataFrame, batch_id: int) -> ...:
        """Fucntion that will be executed on each batch of stream.

        ## Parameters
        `frame` : `pyspark.sql.DataFrame`
            DataFrame to execute on.
        `batch_id` : `int`
            Batch id.
        """
        log.info(f"Excecuting function for {batch_id=}")

        frame.persist(StorageLevel.MEMORY_ONLY)

        count: int = frame.count()
        offsets_per_trigger: int = int(config["trigger"]["offsets-per-trigger"])

        log.info(f"Processed {count=} rows")
        if count != offsets_per_trigger -1: 
            log.warning(
                f"Processed row not equal to configured offsets per trigger! {count=} {offsets_per_trigger=}"
            )
        
        frame = (
            frame
                .withColumns(dict(
                    date=F.date(F.col('trigger_dttm')), #TODO AttributeError: module 'pyspark.sql.functions' has no attribute 'date'
                    hour=F.hour(F.col('trigger_dttm')),
                    batch_id=F.lit(batch_id),
                ))
        )

        if mode == "dev":
            frame.show(100)
            frame.printSchema()

        _write_dataframe(
            frame=frame.where(F.col("object_type") == "currency"),
            path=f"{config['output-path']}/object_type=currency/date={TODAY.date().__str__()}/hour={TODAY.time().hour}:00/batch_id={batch_id}",
        )

        _write_dataframe(
            frame=frame.where(F.col("object_type") == "transaction"),
            path=f"{config['output-path']}/object_type=transaction/date={TODAY.date().__str__()}/hour={TODAY.time().hour}:00/batch_id={batch_id}",
        )

        frame.unpersist()

    match mode:
        case "dev":
            frame.explain(mode="formatted")

            return (
                frame.writeStream.queryName(config["query-name"])
                .trigger(processingTime=config["trigger"]["processing-time"])
                .foreachBatch(func=_foreach_batch_func)
                .options(
                    truncate=False,
                    checkpointLocation=f'{config["checkpoint-location"]}/{config["app-name"]}/{config["query-name"]}',
                )
            )
        case "test":
            return (
                frame.writeStream.queryName(config["query-name"])
                .trigger(processingTime=config["trigger"]["processing-time"])
                .foreachBatch(func=_foreach_batch_func)
                .options(
                    checkpointLocation=f'{config["checkpoint-location"]}/{config["app-name"]}/{config["query-name"]}',
                )
            )
        case "prod":
            return (
                frame.writeStream.queryName(config["query-name"])
                .trigger(processingTime=config["trigger"]["processing-time"])
                .foreachBatch(func=_foreach_batch_func)
                .options(
                    checkpointLocation=f'{config["checkpoint-location"]}/{config["app-name"]}/{config["query-name"]}',
                )
            )


def _read_stream(
    spark: pyspark.sql.SparkSession, config: dict
) -> pyspark.sql.DataFrame:
    """Read kafka topic and return batched DataFrame.

    Tagret topic and other options should be speified in project config file `config.yaml`.

    Kafka connection and security options should be set as env variables. For more information see `.env.template`

    ## Parameters
    `spark` : `pyspark.sql.SparkSession`
        Active Spark Session.
    `config` : `dict`
        Dict with job configurations.

    ## Returns
    `pyspark.sql.DataFrame`
        Batched DataFrame.
    """
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
    log.info(f"Reading stream with given config -> {config=}")

    options = {
        "kafka.bootstrap.servers": BOOTSTRAP_SERVER,
        "startingOffsets": "earliest",
        "subscribe": config["topic"],
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "SCRAM-SHA-512",
        "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{USERNAME}" password="{PASSWORD}";',
        "kafka.ssl.truststore.type": "PEM",
        "kafka.ssl.truststore.location": CERTIFICATE_PATH,
        "maxOffsetsPerTrigger": config["trigger"]["offsets-per-trigger"],
    }

    log.info(
        f"Subscribe to {config['topic']} kafka topic. Will consume {config['trigger']['offsets-per-trigger']} offsets per one trigger"
    )

    frame = (
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
            .withColumns(
                dict(
                    object_id=F.col("value.object_id"),
                    object_type=F.col("value.object_type"),
                    sent_dttm=F.col("value.sent_dttm"),
                    payload=F.col("value.payload"),
                )
        )
            .withColumns(
                dict(
                    sent_dttm=F.to_timestamp(F.regexp_replace(F.col("sent_dttm"), "T", " "), r"yyyy-MM-dd HH:mm:ss"),
                    object_type=F.lower(F.col("object_type")),
                    trigger_dttm=F.lit(datetime.now())
                )
            )
    )

    return (
        frame
            .drop_duplicates(
                subset=config['deduplicate']['cols-subset']
            )
            .withWatermark(
                eventTime="trigger_dttm",
                delayThreshold=config['deduplicate']['delay-threshold']
            )
        )



def _write_dataframe(frame: pyspark.sql.DataFrame, path: str) -> ...:
    """Write given DataFrame to S3 path.

    ## Parameters
    `frame` : `pyspark.sql.DataFrame`
        DataFrame to write.
    `path` : `str`
        S3 path.
    """

    log.info(f"Wriring dataframe to {path=} path")

    try:
        # Raising error if target path already exists
        frame.write.parquet(path=path, mode="errorifexists", compression="gzip")
        log.info(f"Done! Results -> {path=}")

    except AnalysisException as err:
        # If path exist logging warning and appending 
        log.warning(f"Notice that {str(err)}")

        frame.write.parquet(path=path, mode="append", compression="gzip")
        log.info(f"Done! Results -> {path=}")
