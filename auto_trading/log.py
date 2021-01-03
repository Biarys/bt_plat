import logging
import os
from Backtest import Settings as settings

def setup_log(name, level=logging.INFO):
    _create_folders()

    logger = logging.getLogger(name)
    logger.setLevel(level)
    # formatter = logging.Formatter("%(levelname)s - %(asctime)s - %(name)s - %(threadName)s - %(message)s")
    formatter = logging.Formatter("%(levelname)s - %(asctime)s - %(name)s - %(message)s")

    handler_console = logging.StreamHandler()
    handler_console.setLevel(level)
    handler_console.setFormatter(formatter)

    handler_file = logging.FileHandler(settings.log_folder + r"/" + settings.log_name, mode="w")
    handler_file.setLevel(level)
    handler_file.setFormatter(formatter)
    
    logger.addHandler(handler_console)
    logger.addHandler(handler_file)

    logger.info(f"{name} started")

def _create_folders():
    for path in ["/", "/INFO", "/ERRORS", "DEBUG"]:
        if not os.path.exists(settings.log_folder+path):
            try:
                print(f"Creating log folder in {settings.log_folder+path}")
                os.mkdir(settings.log_folder)
                os.mkdir(settings.log_folder+path)
                os.mkdir(settings.log_folder+path)
                os.mkdir(settings.log_folder+path)
            except Exception as e:
                print(f"Failed to create log folder in {settings.log_folder+path}")
                print(f"An error occured {e}")
