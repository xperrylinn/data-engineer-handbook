import logging
import sys

# Create a logger instance
logger = logging.getLogger("4-apache-flink-training")
logger.setLevel(logging.INFO)  # Set the default logging level

# Create a StreamHandler to output logs to stdout
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)

# Define a simple log format
formatter = logging.Formatter(
    fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
stream_handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(stream_handler)
