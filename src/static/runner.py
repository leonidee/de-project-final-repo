from __future__ import annotations

import sys
from os import getenv

import dotenv
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException, CapturedException

dotenv.load_dotenv()

sys.path.append(getenv("APP_PATH"))
from src.logger import get_logger

log = get_logger(__name__)

with open(f'{getenv("APP_PATH")}/config.yaml') as f:
    config = yaml.safe_load(f)["stream"]


def read_data(spark):
    df = spark.read.parquet(config["output-path"])

    df.printSchema()

    df.show(100, False)


def main() -> None:
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
        read_data(spark=spark)
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
