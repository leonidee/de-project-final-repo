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


def give_me_name(processing_dttm: datetime | None = None):
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
        operation="""
            COPY LEONIDGRISHENKOVYANDEXRU__STAGING.transaction_temp
                FROM
                    's3a://data-ice-lake-05/dev/clean/transaction-service/transaction/date=2023-10-02/hour=16/*'
                    PARQUET;
        """
    )

    query = """ 
        SELECT count(*)
        FROM LEONIDGRISHENKOVYANDEXRU__STAGING.transaction_temp
    """

    data = cur.execute(query).fetchone()

    print(data)
