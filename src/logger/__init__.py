import sys
import warnings
from logging import Formatter, Logger, StreamHandler, getLogger
from os import getenv

DEFAULT_LOGGING_LEVEL = "INFO"
DEFAULT_LOGGING_HANDLER = "console"


def get_logger(name: str) -> Logger:
    """Gets configured Logger instance.

    Logger level and handler should be specified via environment variables: `LOGGING_LEVEL` and `LOGGING_HANDLER`.

    ## Parameters
    `name` : `str`
        Name of the logger

    ## Returns
    `logging.Logger`
    """

    level = getenv("LOGGING_LEVEL")
    handler = getenv("LOGGING_HANDLER")

    if not level:
        warnings.warn(
            message="Logging level is not specified. "
            f"Default value will be used -> {DEFAULT_LOGGING_LEVEL}. "
            "If you want to change set level as environment variable LOGGING_LEVEL",
            category=RuntimeWarning,
        )

        level: str = DEFAULT_LOGGING_LEVEL

    if not handler:
        warnings.warn(
            message="Logging handler is not specified. "
            f"Default value will be used -> {DEFAULT_LOGGING_HANDLER}. "
            "If you want to change set handler as environment variable LOGGING_HANDLER",
            category=RuntimeWarning,
        )
        handler = DEFAULT_LOGGING_HANDLER

    handler = handler.strip().lower()
    match handler:
        case "console":
            handler = StreamHandler(stream=sys.stdout)
        case _:
            raise ValueError(
                "Please specify correct handler for logging object as LOGGING_HANDLER variable"
            )

    level = level.strip().lower()
    match level:
        case "debug" | "info" | "notset":
            message_format = r"[%(asctime)s] {%(name)s.%(funcName)s:%(lineno)d} %(levelname)s: %(message)s"
        case "warning" | "error":
            message_format = (
                r"[%(asctime)s] {%(name)s.%(lineno)d} %(levelname)s: %(message)s"
            )
        case "critical":
            message_format = r"[%(asctime)s] {%(name)s} %(levelname)s: %(message)s"
        case _:
            raise ValueError(
                "Please specify correct level for logging object as LOGGING_LEVEL variable"
            )

    logger = getLogger(name=name)

    if logger.hasHandlers():
        logger.handlers.clear()

    logger.setLevel(level=level.upper())

    handler.setFormatter(
        fmt=Formatter(
            fmt=message_format,
            datefmt=r"%Y-%m-%d %H:%M:%S",
        )
    )

    logger.addHandler(handler)

    logger.propagate = False

    return logger
