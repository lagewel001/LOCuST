from abc import ABC, abstractmethod
from typing import List, Optional, Tuple

from utils.custom_types import BaseModel


class BaseGenerator(BaseModel, ABC):
    """Main abstract base model for doing query generation."""
    @abstractmethod
    def __init__(self):
        pass

    def generate_query(self, question: str, golden_tables: Optional[List[str]] = None) -> Tuple[str, int]:
        """
            Generate a logical query for the given question.

            :param question: natural language question
            :param golden_tables: list of golden table IDs for the query-only generation task
            :return: tuple containing the logical SQL or S-expression string and number of output tokens
        """
        raise NotImplementedError()
