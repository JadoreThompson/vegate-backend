from ..error import ValidationError
from .base import Validator, LoadedData


class TrafaretValidator(Validator):
    """Validator class for Trafaret library."""

    def load(self, data: LoadedData, **params):
        try:
            return self.schema.check(data)
        except Exception as e:
            raise ValidationError(origin=e, reason=str(e))
