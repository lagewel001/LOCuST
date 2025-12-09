import logging
import os
from datetime import datetime
from pythonjsonlogger.jsonlogger import JsonFormatter
from tqdm import tqdm
from typing import Dict


class TqdmLoggingHandler(logging.Handler):
    """
        Class for enabling logging during a process with a tqdm progress bar.
        Using this handler logs will be put above the progress bar, pushing the
        process bar down instead of replacing it.
    """
    def __init__(self, level: int = logging.DEBUG if os.getenv('ENV', 'local') == 'local' else logging.INFO):
        super().__init__(level)

    def emit(self, record: logging.LogRecord):
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            self.handleError(record)


class ElkJsonFormatter(JsonFormatter):
    def add_fields(self, log_record: Dict, record: logging.LogRecord, message_dict: Dict[str, str]):
        """
            Add fields to the json-formatted message
        """
        super(ElkJsonFormatter, self).add_fields(log_record, record, message_dict)
        log_record['@timestamp'] = datetime.now().isoformat()
        log_record['level'] = record.levelname
        log_record['logger'] = record.name
