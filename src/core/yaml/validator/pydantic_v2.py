from ..error import ValidationError
from .base import Validator, LoadedData


class PydanticV2Validator(Validator):
    """Validator class for Pydantic Version 2."""

    def load(self, data: LoadedData, **params):
        try:
            return self.schema(**data).model_dump()
        except Exception as e:
            raise ValidationError(origin=e, reason=str(e))
