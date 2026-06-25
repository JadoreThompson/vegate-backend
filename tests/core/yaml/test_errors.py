import pytest

from core.yaml.error import ConfigError, LoadingError, PinyErrorMixin, ValidationError


class TestPinyErrorMixin:
    def test_msg_template_is_formatted_with_context(self):
        class TestError(PinyErrorMixin, Exception):
            msg_template = "Failed: {reason}"

        exc = TestError(reason="something broke")
        assert str(exc) == "Failed: something broke"

    def test_origin_is_stored(self):
        class TestError(PinyErrorMixin, Exception):
            msg_template = "err"

        inner = ValueError("inner")
        exc = TestError(origin=inner, reason="x")
        assert exc.origin is inner
        assert exc.context == {"reason": "x"}


class TestConfigError:
    def test_is_subclass_of_piny_error_mixin_and_exception(self):
        assert issubclass(ConfigError, PinyErrorMixin)
        assert issubclass(ConfigError, Exception)

    def test_can_be_raised_without_args(self):
        with pytest.raises(ConfigError):
            raise ConfigError()


class TestLoadingError:
    def test_msg_template(self):
        exc = LoadingError(reason="file not found")
        assert str(exc) == "Loading YAML file failed: file not found"

    def test_origin_is_preserved(self):
        inner = FileNotFoundError("no such file")
        exc = LoadingError(origin=inner, reason=str(inner))
        assert exc.origin is inner


class TestValidationError:
    def test_msg_template(self):
        exc = ValidationError(reason="invalid schema")
        assert str(exc) == "Validation failed: invalid schema"
