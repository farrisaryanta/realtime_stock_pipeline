import logging
import os
import sys
import config

def get_logger(logger_name: str) -> logging.Logger:
    """
    Creates and configures a standardized logger for the project.
    Outputs logs to both the console (terminal) and a physical log file.
    
    Args:
        logger_name (str): The name of the module calling the logger (usually __name__).
        
    Returns:
        logging.Logger: A configured logger instance.
    """
    # Ensure the directory for log files exists before writing to it
    os.makedirs(config.LOG_DIR, exist_ok=True)

    # Initialize the logger
    logger = logging.getLogger(logger_name)
    
    # Set the log level dynamically based on the configuration
    log_level = getattr(logging, config.LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(log_level)

    # Prevent duplicate log entries if the logger is called multiple times
    if not logger.handlers:
        # Define a clear, readable format for the logs
        log_format = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
        date_format = "%Y-%m-%d %H:%M:%S"
        formatter = logging.Formatter(fmt=log_format, datefmt=date_format)

        # 1. File Handler: Saves logs to the specified file (e.g., logs/pipeline.log)
        file_handler = logging.FileHandler(config.LOG_FILE)
        file_handler.setFormatter(formatter)

        # 2. Stream Handler: Prints logs to the console for real-time monitoring
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)

        # Attach both handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger