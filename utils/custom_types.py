"""
This utility module contains custom types and dataclasses that can be used for validating
parameter input in functions when using beartype or general convenience in providing type hints.
"""
import operator
from abc import ABC, abstractmethod
from beartype.vale import Is
from enum import StrEnum
from dataclasses import dataclass
from typing import Annotated, List, TypeVar, Literal

T = TypeVar('T')

NonEmpty = Is[lambda lst: len(lst) > 0]
NonEmptyList = Annotated[List[T], NonEmpty]

ComparisonOperator = Literal['<', '>', '!=', '<=', '>=', '=']

Connective = Literal['AND', 'OR']

COMPARISON_OPERATORS = {
    '<': operator.lt,
    '>': operator.gt,
    '!=': operator.ne,
    '<=': operator.le,
    '>=': operator.ge,
    '=': operator.eq
}

QueryType = Literal['sexp', 'sql', 'pivot_sql']


class FormatWarning(Warning):
    """Raised when SQL output of a query is not correctly pivoted to our rules."""
    pass


class UnitCompatibilityError(TypeError):
    """Raised when units are not compatible for an aggregation operation."""
    pass


class PartitionCoherenceError(ValueError):
    """
        Raised when an aggregation collapses a dimension over an incoherent code-set: codes
        spanning more than one partition (dimension group), or a total/aggregate code mixed in
        with sibling codes. Such aggregations double-count and must be rewritten explicitly.
    """
    pass


@dataclass
class QAPair(object):
    question: str
    sexp: str
    sql: str
    pivot_sql: str

    def __getitem__(self, attr: str):
        return getattr(self, attr)


class BaseModel(ABC):
    """Main abstract base model."""
    @abstractmethod
    def __init__(self):
        pass


class GraphEngineKind(StrEnum):
    LOCAL = 'local'
    GRAPHDB = 'graphdb'
    VIRTUOSO = 'virtuoso'


class LLMAnswerStatus(StrEnum):
    OK = 'OK'
    UNANSWERABLE = 'UNANSWERABLE'
    ALTERNATIVE = 'ALTERNATIVE'


@dataclass
class LLMPrompt(object):
    """A class that holds the system and user prompts for downstream tasks."""
    system_prompt: str
    user_prompt: str


@dataclass
class LLMResponse(object):
    query: str
    input_token_count: int
    output_token_count: int
    status: LLMAnswerStatus
    remarks: str

    def to_dict(self):
        return {
            'query': self.query,
            'input_token_count': self.input_token_count,
            'output_token_count': self.output_token_count
        }

    def get(self, attr: str, default=None):
        return getattr(self, attr, default)
