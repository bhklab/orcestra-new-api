# api/logger_config.py
import logging
import colorlog
import os

def setup_logger():
    """Configures the global logging setup for the application."""

    log_directory = "api/logs"
    log_filename = "application.log"
    log_filepath = os.path.join(log_directory, log_filename)
    os.makedirs(log_directory, exist_ok=True)

    # Avoid duplicate handlers if setup_logger() is called more than once
    if logging.getLogger().handlers:
        return

    # Handlers
    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(log_filepath)

    # Formatters
    console_formatter = colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'bold_green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'bold_red,bg_white',
        }
    )
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    console_handler.setFormatter(console_formatter)
    file_handler.setFormatter(file_formatter)

    # Root logger configuration
    logging.basicConfig(level=logging.INFO, handlers=[console_handler, file_handler])