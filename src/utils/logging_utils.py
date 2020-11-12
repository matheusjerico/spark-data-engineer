import logging
from pythonjsonlogger import jsonlogger

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
json_handler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(
    fmt='%(asctime)s %(levelname)s %(message)s'
)
json_handler.setFormatter(formatter)
logger.addHandler(json_handler)

def log(level: str, message):
    if level == 'INFO':
        logger.info(message)
    elif level == 'CRITICAL':
        logger.critical(message)
    elif level == 'ERROR':
        logger.error(message)
    elif level == 'WARNING':
        logger.warning(message)
    else:
        logger.debug(message)
    
    
