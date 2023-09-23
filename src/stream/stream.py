from __future__ import annotations

import sys
from os import getenv

import click
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException, CapturedException

sys.path.append("/app")
from src.logger import get_logger
from src.stream.components import Query

log = get_logger(__name__)


@click.command()
@click.option("--mode", help="Some description", type=str)
def main(mode: str) -> None:
    spark = (
        SparkSession.builder.master("spark://spark-master:7077")
        .appName("test")
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

    query = Query(spark=spark, mode=mode).get_query()

    try:
        query.start().awaitTermination()
    except (CapturedException, AnalysisException) as err:
        log.exception(err)
        query.stop()  # type:ignore
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        log.exception(err)
        sys.exit(2)
