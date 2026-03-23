# dag_engine/logger.py

import logging
import sys
from enum import IntEnum


class LogLevel(IntEnum):
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL


def get_logger(name: str, level: LogLevel = LogLevel.DEBUG) -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(level.value)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level.value)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False

    return logger


# Module-level loggers
engine_logger = get_logger("dag.engine")
graph_logger = get_logger("dag.graph")
state_logger = get_logger("dag.state")
node_logger = get_logger("dag.node")
api_logger = get_logger("dag.api")