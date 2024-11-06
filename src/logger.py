import logging
import os
from datetime import datetime

def setup_logger():
    """Configure and return the application logger"""
    # Create logs directory if it doesn't exist
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)

    # Create a logger
    logger = logging.getLogger('RepoGuardian')
    logger.setLevel(logging.INFO)

    # Create handlers
    log_file = os.path.join(log_dir, f'repo_guardian_{datetime.now().strftime("%Y%m%d")}.log')
    file_handler = logging.FileHandler(log_file)
    console_handler = logging.StreamHandler()

    # Create formatters and add it to handlers
    log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(log_format)
    console_handler.setFormatter(log_format)

    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

# Create a global logger instance
logger = setup_logger()
