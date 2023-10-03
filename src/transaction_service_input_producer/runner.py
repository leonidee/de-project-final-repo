import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import click
import dotenv

sys.path.append(os.getenv("APP_PATH"))
from src import get_logger
from src.transaction_service_input_producer import processor

log = get_logger(__name__)

dotenv.load_dotenv()


@click.command()
@click.option(
    "--mode",
    help="Job submition mode",
    type=click.Choice(["PROD", "TEST", "DEV"], case_sensitive=True),
    required=True,
)
def main(mode: str) -> None:
    log.info("Running data producer application")
    stopwatch = datetime.now()

    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(processor.produce_currency_data, mode)
        executor.submit(processor.produce_transaction_data, mode)

    log.info(f"Done! Its took: {datetime.now() - stopwatch}")


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        log.exception(err)
        sys.exit(2)
