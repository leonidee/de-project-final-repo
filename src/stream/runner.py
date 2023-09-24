from __future__ import annotations

import sys
from os import getenv

import click
import dotenv
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException, CapturedException

dotenv.load_dotenv()

sys.path.append(getenv("APP_PATH"))
from src.logger import get_logger
from src.stream.config import parse_config
from src.stream.query import get_query

log = get_logger(__name__)


@click.command()
@click.option(
    "--mode",
    help="Job submition mode",
    type=click.Choice(["prod", "test", "dev"], case_sensitive=True),
    required=True,
)
def main(mode: str) -> None:
    config = parse_config(mode=mode)

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

    query = get_query(spark=spark, mode=mode)

    try:
        query.start().awaitTermination()
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
