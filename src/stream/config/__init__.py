from os import getenv

import yaml

from src.logger import get_logger

log = get_logger(__name__)

CONFIG_PATH = f"{getenv('APP_PATH')}/config.yaml"
NAME = "stream"


def parse_config(mode: str) -> dict[str, str | int]:
    """Parse config file `config.yaml` for `stream` job.

    ## Parameters
    `mode` : `str`
        Which options to parse.

    ## Returns
    `dict`
        Configuration represented as dict.
    """
    log.debug(f"Opening {CONFIG_PATH=} with {mode=}")

    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)[NAME]

    match mode:
        case "dev":
            return config["dev"]
        case "prod":
            return config["prod"]
        case "test":
            return config["prod"]
