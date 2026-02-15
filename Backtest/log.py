import logging
import logging.config
import os
from datetime import datetime as dt
from Backtest.settings import Settings as settings

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {"format": "%(levelname)s - %(asctime)s - %(name)s - %(message)s"},
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
            "level": "INFO",
            "stream": "ext://sys.stdout",
        },
        "file": {
            "class": "logging.FileHandler",
            "formatter": "default",
            "level": "DEBUG",
            "filename": settings.log_folder + "/" + dt.now().strftime("%d_%b_%Y_asof_%H_%M_%S.log"),
            "mode": "w",
        }
    },
    "root": {"level": "INFO", "handlers": ["console", "file"]},
}


logger = logging.getLogger(__name__)

def setup_log():
    _create_log_folders()

    logging.config.dictConfig(LOGGING_CONFIG)
    logger.info("Logging is set up.")

def _create_log_folders():
    for path in ["/", "/INFO", "/ERRORS", "/DEBUG"]:
        if not os.path.exists(settings.log_folder+path):
            try:
                print(f"Creating log folder in {settings.log_folder+path}")
                os.mkdir(settings.log_folder+path)
            except Exception as e:
                print(f"Failed to create log folder in {settings.log_folder+path}")
                print(f"An error occured {e}")
