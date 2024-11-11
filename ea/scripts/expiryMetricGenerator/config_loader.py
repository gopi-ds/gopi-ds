import json5
import logging
import os

def load_config(config_path):
    try:
        with open(config_path, 'r') as config_file:
            config = json5.load(config_file)
            return config
    except Exception as e:
        print(f"Failed to load configuration file: {e}")
        raise

def setup_logging(logging_config):
    log_file = logging_config.get("log_file")
    log_level = logging_config.get("log_level", "INFO").upper()
    log_format = logging_config.get("log_format", "%(asctime)s - %(levelname)s - %(message)s")

    try:
        # Create log file directory if it does not exist
        log_dir = os.path.dirname(log_file) if log_file else None
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        # Set up logging with both console and file handlers
        logging.basicConfig(
            level=getattr(logging, log_level, logging.INFO),
            format=log_format,
            handlers=[
                logging.FileHandler(log_file) if log_file else logging.NullHandler(),  # File handler if log_file provided
                logging.StreamHandler()  # Console handler for logging to stdout
            ]
        )
        logging.info("Logging configured successfully with level: %s and file: %s", log_level, log_file or "None (console only)")
    except Exception as e:
        print(f"Failed to configure logging: {e}")
        raise

def ensure_directories(app_settings):
    try:
        # Ensure output directory exists
        output_dir = app_settings.get("output_dir")
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
            logging.info(f"Output directory ensured at: {output_dir}")

        # Ensure input directory exists
        input_dir = app_settings.get("input_dir")
        if input_dir:
            os.makedirs(input_dir, exist_ok=True)
            logging.info(f"Input directory ensured at: {input_dir}")
    except Exception as e:
        logging.error(f"Failed to ensure directories: {e}")
        raise

def load_and_configure(config_path):
    # Load configuration
    config = load_config(config_path)

    # Set up logging
    setup_logging(config.get("Logging", {}))
    logging.info(f"Configuration loaded from {config_path}")

    # Ensure directories exist
    ensure_directories(config.get("AppSettings", {}))

    return config
