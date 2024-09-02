import logging
import os

class Logger:
    def __init__(self, log_file):
        # Ensure the log directory exists
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        # Set up the logging configuration
        logging.basicConfig(
            filename=log_file,
            filemode='a',  # Append mode, use 'w' to overwrite
            format='%(asctime)s - %(levelname)s - %(message)s',
            level=logging.DEBUG
        )
        self.logger = logging.getLogger()

    def log(self, message, level="info"):
        """
        Log a message with the specified level.
        Levels: 'info', 'warning', 'error', 'critical', 'debug'
        """
        level = level.lower()
        if level == "info":
            self.logger.info(message)
        elif level == "warning":
            self.logger.warning(message)
        elif level == "error":
            self.logger.error(message)
        elif level == "critical":
            self.logger.critical(message)
        elif level == "debug":
            self.logger.debug(message)
        else:
            raise ValueError(f"Unsupported log level: {level}")

    def log_exception(self, message):
        """
        Log an exception with traceback.
        """
        self.logger.exception(message)