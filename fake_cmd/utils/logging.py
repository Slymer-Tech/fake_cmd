import logging


def create_logger(name: str) -> logging.Logger:
    """
    Create logger and return.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            f'[{name} {{levelname}}] - {{asctime}} - "{{filename}}:{{lineno}}" - {{message}}',
            datefmt="%Y/%m/%d %H:%M:%S",
            style="{",
        )
    )
    logger.addHandler(handler)
    return logger


logger = create_logger("fake_cmd")
