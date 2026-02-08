import logging
from types import MethodType
from logging.handlers import RotatingFileHandler
from pathlib import Path


class HumanFormatter(logging.Formatter):
    def format(self, record):
        if getattr(record, "raw_only", False):
            return record.getMessage()

        timestamp = self.formatTime(record, self.datefmt)
        message = (
            f"[lattice] {timestamp} {record.levelname.lower()} "
            f"{record.name} {record.getMessage()}"
        )
        if hasattr(record, "extra"):
            message = f"{message} | data={record.extra}"
        return message


def setup_logger(name: str, logfile: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    log_path = Path(logfile)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    if logger.handlers:
        return logger

    handler = RotatingFileHandler(
        str(log_path),
        maxBytes=5 * 1024 * 1024,
        backupCount=5
    )
    formatter = HumanFormatter(
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.addHandler(stream_handler)
    logger.propagate = False

    def rawlog(self, message: str):
        self.info(message, extra={"raw_only": True})

    logger.rawlog = MethodType(rawlog, logger)
    return logger
