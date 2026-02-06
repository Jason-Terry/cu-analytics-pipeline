import logging
import time
from contextlib import contextmanager
from enum import Enum


class LogLevel(Enum):
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL


class Logger:
    def __init__(self, name: str, level: LogLevel = LogLevel.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level.value)

        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def debug(self, message: str):
        self.logger.debug(message)

    def info(self, message: str):
        self.logger.info(message)

    def warning(self, message: str):
        self.logger.warning(message)

    def error(self, message: str):
        self.logger.error(message)

    def critical(self, message: str):
        self.logger.critical(message)

    @contextmanager
    def timed(self, label: str):
        """Context manager that logs elapsed time for a block of code.

        Usage:
            with logger.timed("Claude API call"):
                result = call_claude(...)
        """
        start = time.perf_counter()
        yield
        elapsed_ms = (time.perf_counter() - start) * 1000
        if elapsed_ms >= 1000:
            self.info(f"{label} done in {elapsed_ms / 1000:.2f}s")
        else:
            self.info(f"{label} done in {elapsed_ms:.0f}ms")
