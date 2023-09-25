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


def collect_transaction_table(
    spark: pyspark.sql.SparkSession, mode: str, date: str, hour: str
) -> None:
    log.info(f"Collectin transaction DataFrame with {mode=} {date=} {hour=}")

    config = parse_config(app="transaction-service-clean-collector", mode=mode)

    log.info(f"Input data path: {config['input-path']}")

    frame = (
        spark.read.parquet(config["input-path"])
        .where(
            (F.col("object_type") == "transaction")
            & (F.col("date") == date)
            & (F.col("hour") == hour)
        )
        .select("object_id", "sent_dttm", "payload")
    )

    frame = (
        frame.withColumn(
            "payload",
            F.from_json(
                F.col("payload"),
                schema=T.StructType(
                    [
                        T.StructField("operation_id", T.StringType(), True),
                        T.StructField("account_number_from", T.DoubleType(), True),
                        T.StructField("account_number_to", T.DoubleType(), True),
                        T.StructField("currency_code", T.IntegerType(), True),
                        T.StructField("country", T.StringType(), True),
                        T.StructField("status", T.StringType(), True),
                        T.StructField("transaction_type", T.StringType(), True),
                        T.StructField("transaction_dt", T.StringType(), True),
                    ]
                ),
            ),
        )
        .withColumns(
            dict(
                operation_id="payload.operation_id",
                account_number_from="payload.account_number_from",
                account_number_to="payload.account_number_to",
                currency_code="payload.currency_code",
                country="payload.country",
                status="payload.status",
                transaction_type="payload.transaction_type",
                transaction_dt=F.to_timestamp(
                    F.col("payload.transaction_dt"), r"yyyy-MM-dd HH:mm:ss"
                ),
            )
        )
        .drop_duplicates(subset=["operation_id", "transaction_dt", "status"])
        .select(
            "operation_id",
            "account_number_to",
            "currency_code",
            "country",
            "status",
            "transaction_type",
            "transaction_dt",
        )
    )
    # todo в стрименге дропать дубликаты с watermart по полям send_dttm и objet_id ?
    frame.show(100, False)

    log.info(f"ROWS COUNT ---> {frame.count()}")

    frame.explain(mode="formatted")


def collect_currency_table(
    spark: pyspark.sql.SparkSession, mode: str, date: str, hour: str
) -> None:
    log.info(f"Collecting currency DataFrame with {mode=} {date=} {hour=}")

    config = parse_config(app="transaction-service-clean-collector", mode=mode)

    log.info(f"Input data path: {config['input-path']}")

    frame = (
        spark.read.parquet(config["input-path"])
        .where(
            (F.col("object_type") == "currency")
            & (F.col("date") == date)
            & (F.col("hour") == hour)
        )
        .select("object_id", "sent_dttm", "payload")
    )

    frame = frame.withColumn(
        "payload",
        F.from_json(
            F.col("payload"),
            schema=T.StructType(
                [
                    T.StructField("date_update", T.StringType(), True),
                    T.StructField("currency_code", T.IntegerType(), True),
                    T.StructField("currency_code_with", T.IntegerType(), True),
                    T.StructField("currency_with_div", T.FloatType(), True),
                ]
            ),
        ),
    ).withColumns(
        dict(
            date_update=F.to_timestamp(
                F.col("payload.date_update"), r"yyyy-MM-dd HH:mm:ss"
            ),
            currency_code="payload.currency_code",
            currency_code_with="payload.currency_code_with",
            currency_with_div="payload.currency_with_div",
        )
    )

    frame.show(100, False)

    log.info(f"ROWS COUNT ---> {frame.count()}")

    frame.explain(mode="formatted")
