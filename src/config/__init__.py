from os import getenv

import yaml

from src.logger import get_logger

log = get_logger(__name__)

CONFIG_PATH = f"{getenv('APP_PATH')}/config.yaml"


def parse_config(app: str, mode: str) -> dict[str, str | int]:
    """Parse config file `config.yaml` for `stream` job.

    ## Parameters
    `mode` : `str`
        Which options to parse.

    ## Returns
    `dict`
        Configuration represented as dict.
    """
    log.info(f"Parsing config {CONFIG_PATH=} for {app=} with {mode=}")

    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)["apps"]

    match mode:
        case "dev":
            return config[app]["dev"]
        case "prod":
            return config[app]["prod"]
        case "test":
            return config[app]["test"]
