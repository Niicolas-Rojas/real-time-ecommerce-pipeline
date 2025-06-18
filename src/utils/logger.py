# src/utils/logger.py
import logging
import sys

def setup_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)

    # Agregar codificación UTF-8 explícita
    handler.setStream(open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1))

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(handler)

    return logger
