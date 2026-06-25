from ..error import ValidationError
from .base import Validator, LoadedData


class MarshmallowValidator(Validator):
    """Validator class for Marshmallow library."""

    def load(self, data: LoadedData, **params):
        try:
            return self.schema(**self.schema_params).load(data, **params)
        except Exception as e:
            raise ValidationError(origin=e, reason=str(e))
