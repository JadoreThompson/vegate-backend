from abc import ABC, abstractmethod
from typing import Any, Dict, List, Union

from ..error import ValidationError

LoadedData = Union[Dict[str, Any], List[Any]]


class Validator(ABC):
    """Abstract base class for optional validator classes.

    Use only to derive new child classes, implement all abstract methods.
    """

    def __init__(self, schema: Any, **params):
        self.schema = schema
        self.schema_params = params

    @abstractmethod
    def load(self, data: LoadedData, **params):
        """Load data, return validated data or raise an error."""
        pass  # pragma: no cover
