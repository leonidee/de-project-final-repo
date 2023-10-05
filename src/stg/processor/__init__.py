import json
from datetime import datetime
from os import getenv

from src.clients import vertica

S3_ACCESS_KEY_ID = getenv("S3_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = getenv("S3_SECRET_ACCESS_KEY")
S3_ENDPOINT_URL = getenv("S3_ENDPOINT_URL")
S3_REGION = getenv("S3_REGION")

if not all(
    (
        S3_ACCESS_KEY_ID,
        S3_SECRET_ACCESS_KEY,
        S3_ENDPOINT_URL,
        S3_REGION,
    )
):
    raise ValueError(
        "Required environment varialbes not set. See .env.template for more details"
    )

S3_ENDPOINT_URL = S3_ENDPOINT_URL.split("https://")[
    1
]  # Because Vertica expect not URL but just address: storage.yandexcloud.net


def merge_into_transaction(processing_dttm: datetime | None = None):
    conn = vertica.get_connection()

    cur = conn.cursor()

    cur.execute(
        operation=f"""
            ALTER SESSION SET AWSRegion='{S3_REGION}';
            ALTER SESSION SET AWSEndpoint='{S3_ENDPOINT_URL}';
            ALTER SESSION SET AWSAuth='{S3_ACCESS_KEY_ID}:{S3_SECRET_ACCESS_KEY}';
        """
    )

    cur.execute(
        operation="""
            DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__STAGING.transaction_temp;
            CREATE TEMP TABLE LEONIDGRISHENKOVYANDEXRU__STAGING.transaction_temp
            (
                operation_id        varchar(100),
                account_number_from double precision,
                account_number_to   double precision,
                currency_code       bigint,
                country             varchar(100),
                status              varchar(100),
                transaction_type    varchar(100),
                transaction_dt      datetime,
                trigger_dttm        datetime
            )
                ON COMMIT PRESERVE ROWS
                DIRECT
                UNSEGMENTED ALL NODES
                INCLUDE SCHEMA PRIVILEGES;
        """
    )

    cur.execute(
        operation=f"""
            COPY LEONIDGRISHENKOVYANDEXRU__STAGING.transaction_temp
                FROM
                    's3a://data-ice-lake-05/dev/clean/transaction-service/transaction/date={processing_dttm.date()}/hour={processing_dttm.hour}/*'
                    PARQUET;
        """
    )

    cur.execute(
        operation="""
            MERGE
            INTO LEONIDGRISHENKOVYANDEXRU__STAGING.transaction AS tgt
            USING LEONIDGRISHENKOVYANDEXRU__STAGING.transaction_temp AS src
            ON (tgt.operation_id = src.operation_id AND tgt.status = src.status)
            WHEN MATCHED THEN
                UPDATE
                SET
                    account_number_from = src.account_number_from,
                    account_number_to = src.account_number_to,
                    currency_code = src.currency_code,
                    country = src.country,
                    transaction_type = src.transaction_type,
                    transaction_dt = src.transaction_dt,
                    trigger_dttm = src.trigger_dttm,
                    update_dttm = now()
            WHEN NOT MATCHED THEN
                INSERT (operation_id,
                        account_number_from,
                        account_number_to,
                        currency_code,
                        country,
                        status,
                        transaction_type,
                        transaction_dt,
                        trigger_dttm, update_dttm)
                VALUES
                    (src.operation_id, src.account_number_from, src.account_number_to, src.currency_code, src.country, src.status, src.transaction_type,
                    src.transaction_dt,
                    src.trigger_dttm, now());
         """
    )


def merge_into_currency(processing_dttm: datetime) -> None:
    conn = vertica.get_connection()

    cur = conn.cursor()

    cur.execute(
        operation=f"""
            ALTER SESSION SET AWSRegion='{S3_REGION}';
            ALTER SESSION SET AWSEndpoint='{S3_ENDPOINT_URL}';
            ALTER SESSION SET AWSAuth='{S3_ACCESS_KEY_ID}:{S3_SECRET_ACCESS_KEY}';
        """
    )
