from os import getenv
from typing import Literal

import yaml

from src.logger import get_logger

log = get_logger(__name__)


def parse_config(
    app: Literal[
        "transaction-service-stream-collector", "transaction-service-clean-collector"
    ],
    mode: str,
) -> dict[str, str | int]:
    """Parse config file `config.yaml` for `stream` job.

    ## Parameters
    `app` : `Literal`
        Application name which config should be loaded.
    `mode` : `str`
        Which options to parse.

    ## Returns
    `dict`
        Configuration represented as dict.
    """
    log.info(f"Parsing config for {app=} with {mode=}")

    match app:
        case "transaction-service-stream-collector":
            config_path = f"{getenv('APP_PATH')}/src/transaction_service_stream_collector/config.yaml"
        case "transaction-service-clean-collector":
            config_path = f"{getenv('APP_PATH')}/src/transaction_service_clean_collector/config.yaml"

    log.info(f"Loading {config_path=}")

    with open(config_path) as f:
        config = yaml.safe_load(f)["app"]

    match mode:
        case "DEV":
            return config["dev"]
        case "PROD":
            return config["prod"]
        case "TEST":
            return config["test"]
