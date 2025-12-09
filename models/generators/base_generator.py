from abc import ABC, abstractmethod
from typing import List, Optional

from utils.custom_types import BaseModel


class BaseGenerator(BaseModel, ABC):
    """Main abstract base model for doing query generation."""
    @abstractmethod
    def __init__(self):
        pass

    def generate_query(self, question: str, golden_tables: Optional[List[str]] = None) -> str:
        """
            Generate a logical query for the given question.

            :param question: natural language question
            :param golden_tables: list of golden table IDs for the query-only generation task
            :return: logical SQL or S-expression string
        """
        raise NotImplementedError()
