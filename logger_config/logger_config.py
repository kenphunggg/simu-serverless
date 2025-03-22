import logging
from colorama import Fore, Style

# Custom formatter for colored logs
class ColorFormatter(logging.Formatter):
    def format(self, record):
        # Apply colors for standard levels
        if record.levelno == logging.DEBUG:
            record.msg = f"{Fore.YELLOW}{record.msg}{Style.RESET_ALL}"  # Cyan for DEBUG
        elif record.levelno == logging.INFO:
            record.msg = f"{Fore.BLUE}{record.msg}{Style.RESET_ALL}"  # Green for INFO
        elif record.levelno == logging.WARNING:
            record.msg = f"{Fore.LIGHTRED_EX}{record.msg}{Style.RESET_ALL}"  # Yellow for WARNING
        elif record.levelno == logging.ERROR:
            record.msg = f"{Fore.RED}{record.msg}{Style.RESET_ALL}"  # Red for ERROR
        elif record.levelno == logging.CRITICAL:
            record.msg = f"{Fore.MAGENTA}{record.msg}{Style.RESET_ALL}"  # Magenta for CRITICAL
        else:
            # Handle other, undefined cases
            record.msg = f"{Fore.WHITE}{Style.BRIGHT}[OTHER] {record.msg}{Style.RESET_ALL}"  # Bright White with prefix
        return super().format(record)

# Function to configure the logger
def setup_logger():
    logger = logging.getLogger()
    if not logger.hasHandlers():  # Avoid duplicate handlers
        handler = logging.StreamHandler()
        formatter = ColorFormatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
    return logger

# Initialize the logger at import time
setup_logger()