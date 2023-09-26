import sys
from os import getenv

import click
import dotenv
from pyspark.sql import SparkSession

dotenv.load_dotenv()

sys.path.append(getenv("APP_PATH"))
from query import get_query

from src import get_logger, parse_config

log = get_logger(__name__)


@click.command()
@click.option(
    "--mode",
    help="Job submition mode",
    type=click.Choice(["PROD", "TEST", "DEV"], case_sensitive=True),
    required=True,
)
@click.option(
    "--log-level",
    help="Spark logging level",
    type=click.Choice(
        ["ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"],
        case_sensitive=True,
    ),
    required=True,
)
def main(mode: str, log_level: str) -> None:
    config = parse_config(app="transaction-service-stream-collector", mode=mode)

    log.info(
        f"Submitting {config['app-name']} application with {mode=} and {log_level=}"
    )

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
    spark.sparkContext.setLogLevel(log_level.strip().capitalize())

    query = get_query(spark=spark, mode=mode)

    query.awaitTermination()


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        log.exception(err)
        sys.exit(2)
