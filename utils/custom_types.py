"""
This utility module contains custom types and dataclasses that can be used for validating
parameter input in functions when using beartype or general convenience in providing type hints.
"""
from abc import ABC, abstractmethod
from beartype.vale import Is
from dataclasses import dataclass
from typing import Annotated, List, TypeVar, Literal

T = TypeVar('T')

NonEmpty = Is[lambda lst: len(lst) > 0]
NonEmptyList = Annotated[List[T], NonEmpty]

ComparisonOperator = Literal['<', '>', '!=', '<=', '>=', '=']

QueryType = Literal['sexp', 'sql']

class UnitCompatibilityError(TypeError):
    """Raised when units are not compatible for an aggregation operation."""
    pass

@dataclass
class QAPair(object):
    question: str
    sexp: str
    sql: str

    def __getitem__(self, attr: str):
        return getattr(self, attr)


class BaseModel(ABC):
    """Main abstract base model."""
    @abstractmethod
    def __init__(self):
        pass
