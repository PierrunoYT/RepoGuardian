import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler
import os

def setup_logger():
    """Configure and return the application logger with rotation and error logging"""
    # Create logs directory if it doesn't exist
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)

    # Create a logger
    logger = logging.getLogger('RepoGuardian')
    logger.setLevel(logging.INFO)

    # Get log level from environment variable, default to INFO
    log_level = os.getenv('LOG_LEVEL', 'INFO')
    logger.setLevel(getattr(logging, log_level.upper()))

    # Create handlers with rotation
    main_log = os.path.join(log_dir, f'repo_guardian_{datetime.now().strftime("%Y%m%d")}.log')
    error_log = os.path.join(log_dir, f'repo_guardian_error_{datetime.now().strftime("%Y%m%d")}.log')

    # Main log handler (10MB per file, keep 5 backup files)
    file_handler = RotatingFileHandler(
        main_log,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(logging.INFO)

    # Error log handler (10MB per file, keep 5 backup files)
    error_handler = RotatingFileHandler(
        error_log,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    error_handler.setLevel(logging.ERROR)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create formatters and add it to handlers
    log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(log_format)
    error_handler.setFormatter(log_format)
    console_handler.setFormatter(log_format)

    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(error_handler)
    logger.addHandler(console_handler)

    return logger

# Create a global logger instance
logger = setup_logger()
