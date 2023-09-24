from os import getenv
from typing import Literal

import yaml

from src.logger import get_logger

log = get_logger(__name__)


def parse_config(
    app: Literal["transaction-service-stream-collector", "static"], mode: str
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
        case "static":
            config_path = f"{getenv('APP_PATH')}/src/static/config.yaml"

    log.info(f"Loading {config_path=}")

    with open(config_path) as f:
        config = yaml.safe_load(f)["app"]

    match mode:
        case "dev":
            return config["dev"]
        case "prod":
            return config["prod"]
        case "test":
            return config["test"]
