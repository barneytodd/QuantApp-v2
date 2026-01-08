import logging
import sys
from logging import Logger

def get_logger(name: str = __name__) -> Logger:
    """
    Returns a pre-configured logger.
    Structured, deterministic, JSON-friendly output optional.
    """
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        # Avoid duplicate handlers in FastAPI reloads
        return logger

    logger.setLevel(logging.INFO)

    # Console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)

    # Formatter: structured, human-readable
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.propagate = False
    return logger
