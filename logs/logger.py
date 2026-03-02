"""
    This module contains the configuration for all the internal logging.
    If using logging in another module, call
        `from logs import logging
         logger = logging.get_logger(__name__)
         logger.info("foo bar")`
"""
import logging.config
import warnings
from pathlib import Path
from pandas.errors import PerformanceWarning

logging_conf_path = Path(__file__).parent
logging.config.fileConfig(logging_conf_path / 'logging.conf')

logger = logging.getLogger(__name__)

warnings.filterwarnings(action='ignore', category=FutureWarning)
warnings.filterwarnings(action='ignore', category=PerformanceWarning)
warning_level_loggers = ['urllib3', 'requests', 'elasticsearch', 'elastic_transport', 'elastic_transport.transport',
                         'passlib.utils.compat', 'passlib.registry', 'anthropic._base_client', 'httpcore.http11',
                         'httpcore.connection', 'httpx', 'google_genai.models', 'openai._base_client']
for lgr in warning_level_loggers:
    logging.getLogger(lgr).setLevel(logging.WARNING)

