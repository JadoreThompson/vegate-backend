import pytest
from pydantic import BaseModel

from core.yaml.error import ValidationError
from core.yaml.validator import PydanticV2Validator, Validator


class TestValidatorBase:
    def test_abstract_load_raises(self):
        with pytest.raises(TypeError):
            Validator("schema")  # type: ignore[abstract]


class TestPydanticV2Validator:
    def test_validates_and_dumps(self):
        class Schema(BaseModel):
            name: str
            count: int

        validator = PydanticV2Validator(Schema)
        result = validator.load({"name": "test", "count": 3})
        assert result == {"name": "test", "count": 3}

    def test_raises_validation_error_on_mismatch(self):
        class Schema(BaseModel):
            age: int

        validator = PydanticV2Validator(Schema)
        with pytest.raises(ValidationError):
            validator.load({"age": "not-a-number"})

    def test_raises_validation_error_on_missing_field(self):
        class Schema(BaseModel):
            required: str

        validator = PydanticV2Validator(Schema)
        with pytest.raises(ValidationError):
            validator.load({})
