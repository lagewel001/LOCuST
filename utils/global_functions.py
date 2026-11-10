"""
This module contains functions that are used by all other modules in this project.
"""
import json
import importlib.util
import inspect
import logging
import re
import requests
import time
from json import JSONDecodeError
from requests import RequestException
from urllib.parse import urlparse
from urllib.error import URLError
from pathlib import Path
from typing import List, Any, Callable, Union

from utils.custom_types import QAPair, BaseModel, QueryType

logger = logging.getLogger(__name__)

TRUSTED_SOURCES = ['localhost', 'cbs.nl', 'cbscms9', 'rivm.nl', 'politie.nl', 'grensdata.eu', 'volkstellingen.nl']

TRUTHY_STRINGS = ('true', '1', 'yes', 'y', 'on')
FALSY_STRINGS = ('false', '0', 'no', 'n', 'off', '')


class TooManyRequestsError(Exception):
    """Exception when to many requests are received by application."""
    pass


def secure_request(callee: Union[Callable, str], *args, json: bool = True, max_retries=3, verify=True, timeout=1):
    """
        Do a get request by a url or function and do 3 retries in case of non-critical failures.
        Only use this method for background/crawling activities and not for requests in real-time.

        :param callee: url or function to do get request on
        :param args: optional arguments to pass to callee 'getter' function
        :param json: return json or content from request result (default: json)
        :param max_retries: maximum number of tries if request fails before returning None (default: 3)
        :param verify: do request with SSL verification (default: True)
        :param timeout: the allowed number of seconds timeout
        :return: request content or json from request source.
    """
    for _ in range(max_retries):
        try:
            if callable(callee):
                req = callee(*args, timeout=(timeout, timeout), verify=verify)
            elif isinstance(callee, str):
                url = urlparse(callee)
                if re.search(fr"({'|'.join(TRUSTED_SOURCES)})$", url.hostname, re.IGNORECASE) is None:
                    raise URLError(f"{callee} is not in the list of trusted sources {TRUSTED_SOURCES}")

                req = requests.get(callee, timeout=(timeout, timeout), verify=verify)
            else:
                raise RequestException("Callee is not a valid callable or retrievable URL")

            if req.status_code == 429:
                raise TooManyRequestsError
            if req.status_code != 200:
                logger.debug(f"Fetching {callee} succeeded, but yielded status code {req.status_code}")

            data = req.json() if json else req.content
        except (TimeoutError, TooManyRequestsError):
            # Try to fetch again if timed out
            logger.debug(f"Request timed out or too many request for {callee}.")
            time.sleep(3)
            continue
        except JSONDecodeError:
            logger.warning(f"Could not parse JSON-result for request {callee}")
            break
        except RequestException as e:
            logger.error(f"Failed to fetch {callee}: {str(e)}")
            break
        else:
            if callable(callee):
                return req
            return data

    return None


def load_dataset(file_path: str) -> List[QAPair]:
    """Load a train/test dataset containing Question-Answer pairs from a JSONL file."""
    with open(file_path, "r") as f:
        return [QAPair(**json.loads(line)) for line in f]


def load_model_from_path(model_path: str, **kwargs) -> Any:
    """Loads a model from a Python file path. kwargs are passed to the model"""
    path = Path(model_path)
    spec = importlib.util.spec_from_file_location(path.stem, path)
    model_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(model_module)

    for name, obj in inspect.getmembers(model_module):
        if (inspect.isclass(obj)
                and issubclass(obj, BaseModel)
                and not inspect.isabstract(obj)
                and obj.__module__ == model_module.__name__):
            return obj(**kwargs)

    raise TypeError(f"Could not find a class inheriting from BaseModel in {model_path}")


def parse_for_table_id(query: str, query_type: QueryType) -> List[str]:
    """
        Parses an S-expression or SQL query to find the table ID.
        Sexp: The table ID is assumed to be the first argument of the VALUE operator.
        SQL: The table ID is assumed to be part of the parquet file name in the FROM clause.

        :param query: The S-expression/SQL query to parse
        :param query_type: Type of query to parse (default: False = S-expression)
        :returns: A list of table IDs found in the query
    """
    if query_type == 'sexp':
        pattern = r"\(VALUE\s+([A-Z0-9]+)"
    elif query_type in ['sql', 'pivot_sql']:
        pattern = r"FROM\s+['\"](?:[^/]+/)*?([A-Z0-9]+)\.parquet['\"]"
    else:
        raise ValueError(f"Unknown query type: {query_type}")

    table_matches = re.findall(pattern, query, re.IGNORECASE)
    return table_matches


def trim_text(text: str, no_chars: int) -> str:
    return f"{text[:no_chars]}..." if len(text) > no_chars else text


def parse_bool(value: Any) -> bool:
    """
        Parse a value from an argument parser/environment variable from a string/int into a boolean.

        :param value: value to coerce. Non-strings are tested for truthiness as usual.
        :return: the value as a bool
    """
    if not isinstance(value, str):
        return bool(value)

    normalised = value.strip().lower()
    if normalised in TRUTHY_STRINGS:
        return True
    if normalised in FALSY_STRINGS:
        return False
    raise ValueError(f"Cannot interpret {value!r} as a boolean. Use one of {TRUTHY_STRINGS} or {FALSY_STRINGS}.")
