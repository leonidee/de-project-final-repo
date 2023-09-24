from __future__ import annotations

import sys
from os import getenv

import click
import dotenv
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException, CapturedException

dotenv.load_dotenv()

sys.path.append(getenv("APP_PATH"))
from collector import collect_currency_table, collect_transaction_table

from src import get_logger, parse_config

log = get_logger(__name__)


@click.command()
@click.option(
    "--mode",
    help="Job submition mode",
    type=click.Choice(["prod", "test", "dev"], case_sensitive=True),
    required=True,
)
@click.option(
    "--table",
    help="",
    type=click.Choice(["currency", "transaction"], case_sensitive=True),
    required=True,
)
@click.option(
    "--date",
    help="",
    type=str,
    required=True,
)
@click.option(
    "--hour",
    help="",
    type=str,
    required=True,
)
def main(mode: str, table: str, date: str, hour: str) -> None:
    config = parse_config(app="transaction-service-clean-collector", mode=mode)

    spark = (
        SparkSession.builder.master("spark://spark-master:7077")
        .appName(config["app-name"])
        .config(
            map={
                "spark.hadoop.fs.s3a.access.key": getenv("S3_ACCESS_KEY_ID"),
                "spark.hadoop.fs.s3a.secret.key": getenv("S3_SECRET_ACCESS_KEY"),
                "spark.hadoop.fs.s3a.endpoint": getenv("S3_ENDPOINT_URL"),
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            }
        )
        .getOrCreate()
    )

    try:
        match table:
            case "transaction":
                collect_transaction_table(spark=spark, mode=mode, date=date, hour=hour)
            case "currency":
                collect_currency_table(spark=spark, mode=mode, date=date, hour=hour)
    except (CapturedException, AnalysisException) as err:
        log.error(err)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        log.exception(err)
        sys.exit(2)
