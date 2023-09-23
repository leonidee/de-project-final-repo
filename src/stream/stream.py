import sys

import click
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException, CapturedException

sys.path.append("/app")
from src.logger import get_logger
from src.stream.components import get_query

log = get_logger(__name__)


@click.command()
@click.option("--topic", help="Some description", type=str)
def main(topic: str) -> None:
    spark = (
        SparkSession.builder.master("spark://spark-master:7077")
        .appName("test")
        .getOrCreate()
    )

    query = get_query(spark=spark, topic=topic)

    try:
        query.awaitTermination()
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
