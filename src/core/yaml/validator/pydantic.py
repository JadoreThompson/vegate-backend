from ..error import ValidationError
from .base import Validator, LoadedData


class PydanticValidator(Validator):  # pragma: no cover
    """Validator class for Pydantic Version 1."""

    def load(self, data: LoadedData, **params):
        try:
            return self.schema(**data).dict()
        except Exception as e:
            raise ValidationError(origin=e, reason=str(e))
