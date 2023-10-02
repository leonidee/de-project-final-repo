import logging
from os import getenv

import vertica_python

VERTICA_DWH_HOST = getenv("VERTICA_DWH_HOST")
VERTICA_DWH_PORT = getenv("VERTICA_DWH_PORT")
VERTICA_DWH_USER = getenv("VERTICA_DWH_USER")
VERTICA_DWH_PASSWORD = getenv("VERTICA_DWH_PASSWORD")
VERTICA_DWH_DB = getenv("VERTICA_DWH_DB")

if not all(
    [
        VERTICA_DWH_HOST,
        VERTICA_DWH_PORT,
        VERTICA_DWH_USER,
        VERTICA_DWH_PASSWORD,
        VERTICA_DWH_DB,
    ]
):
    raise ValueError(
        "Required environment varialbes not set. See .env.template for more details"
    )


properties = dict(
    host=VERTICA_DWH_HOST,
    port=VERTICA_DWH_PORT,
    user=VERTICA_DWH_USER,
    password=VERTICA_DWH_PASSWORD,
    database=VERTICA_DWH_DB,
    autocommit=False,
    log_level=logging.INFO,
    log_path="",
)


def get_connection() -> vertica_python.Connection:
    return vertica_python.connect(**properties)
