import json
from datetime import datetime
from pprint import pformat

import pandas as pd

from src import get_logger, parse_config
from src.clients import kafka, s3
from src.transaction_service_input_producer.utils import get_uuid, validate_path

log = get_logger(__name__)

s3 = s3.get_client()
producer = kafka.get_producer()


def produce_currency_data(mode: str) -> None:
    config = parse_config(app="transaction-service-input-producer", mode=mode)

    path = config["input-path"]["currency"]
    topic = config["topic"]

    OBJECT_TYPE = "CURRENCY"
    stopwatch = datetime.now()

    if not validate_path(path):
        raise ValueError("Path should be an S3 path")

    log.info(f"Starting producing data for {OBJECT_TYPE=} to {topic} topic")

    with s3.open(path, "rb") as f:
        log.info(f"Getting input data from {path} path")
        frame = pd.read_parquet(f)

    log.info(f"Got dataframe with shape: {frame.shape}")

    count = 1
    for row in frame.itertuples(index=False):
        message = dict(
            object_id=get_uuid(
                obj=(row.currency_code, row.currency_code_with, row.date_update)
            ),
            sent_dttm=row.date_update,
            object_type=OBJECT_TYPE,
            payload=dict(
                date_update=row.date_update,
                currency_code=row.currency_code,
                currency_code_with=row.currency_code_with,
                currency_with_div=row.currency_with_div,
            ),
        )

        log.debug(f"Sending message:\n{pformat(message)}")

        producer.send(topic=topic, value=json.dumps(message).encode("utf-8"))
        count += 1

    log.info(f"Done. Its took: {datetime.now() - stopwatch}. Sent {count} messages")


def produce_transaction_data(mode: str) -> None:
    config = parse_config(app="transaction-service-input-producer", mode=mode)

    path = config["input-path"]["transactions"]
    topic = config["topic"]

    OBJECT_TYPE = "TRANSACTION"
    stopwatch = datetime.now()

    if not validate_path(path):
        raise ValueError("Path should be an S3 path")

    log.info(f"Starting producing data for {OBJECT_TYPE=} to {topic} topic")

    with s3.open(path, "rb") as f:
        log.info(f"Getting input data from {path} path")
        frame = pd.read_parquet(f)

    log.info(f"Got dataframe with shape: {frame.shape}")

    count = 1
    for row in frame.itertuples(index=False):
        message = dict(
            object_id=row.operation_id,
            sent_dttm=row.transaction_dt,
            object_type=OBJECT_TYPE,
            payload=dict(
                operation_id=row.operation_id,
                account_number_from=row.account_number_from,
                account_number_to=row.account_number_to,
                currency_code=row.currency_code,
                country=row.country,
                status=row.status,
                transaction_type=row.transaction_type,
                amount=row.amount,
                transaction_dt=row.transaction_dt,
            ),
        )

        log.debug(f"Sending message:\n{pformat(message)}")

        producer.send(topic=topic, value=json.dumps(message).encode("utf-8"))
        count += 1

    log.info(f"Done. Its took: {datetime.now() - stopwatch}. Sent {count} messages")
