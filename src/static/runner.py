from __future__ import annotations

import sys
from os import getenv

import click
import dotenv
import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException, CapturedException

dotenv.load_dotenv()

sys.path.append(getenv("APP_PATH"))
from src.config import parse_config
from src.logger import get_logger

log = get_logger(__name__)


def read_data(spark: pyspark.sql.DataFrame, config: dict):
    df = spark.read.parquet(config["input-path"])

    df.printSchema()

    df.show(100, False)


def processed_transactions(spark: pyspark.sql.DataFrame, config: dict):
    df = spark.read.parquet(config["input-path"]).where(
        F.col("object_type") == "transaction"
    )

    df.show(100, False)


@click.command()
@click.option(
    "--mode",
    help="Job submition mode",
    type=click.Choice(["prod", "test", "dev"], case_sensitive=True),
    required=True,
)
def main(mode: str) -> None:
    config = parse_config(app="static", mode=mode)

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
        read_data(spark=spark, config=config)
    except (CapturedException, AnalysisException) as err:
        log.error(err)
        query.stop()  # type:ignore
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        log.exception(err)
        sys.exit(2)