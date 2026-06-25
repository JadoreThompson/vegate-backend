import os
import tempfile
from unittest.mock import patch

import pytest
from pydantic import BaseModel

from core.yaml import YamlLoader
from core.yaml.error import LoadingError
from core.yaml.matcher import StrictMatcher
from core.yaml.validator import PydanticV2Validator


class TestYamlLoader:
    def test_loads_simple_yaml(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp:
            tmp.write("key: value\n")
            tmp.flush()
            path = tmp.name

        try:
            loader = YamlLoader(path)
            data = loader.load()
            assert data == {"key": "value"}
        finally:
            os.unlink(path)

    def test_interpolates_env_var(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp:
            tmp.write("key: ${ENV_VAR_FOR_TEST}\n")
            tmp.flush()
            path = tmp.name

        try:
            with patch.dict(os.environ, {"ENV_VAR_FOR_TEST": "resolved"}, clear=True):
                loader = YamlLoader(path)
                data = loader.load()
            assert data == {"key": "resolved"}
        finally:
            os.unlink(path)

    def test_raises_loading_error_on_missing_file(self):
        loader = YamlLoader("/tmp/nonexistent_file_xyz.yaml")
        with pytest.raises(LoadingError):
            loader.load()

    def test_raises_loading_error_on_invalid_yaml(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp:
            tmp.write(": invalid yaml [[[\n")
            tmp.flush()
            path = tmp.name

        try:
            loader = YamlLoader(path)
            with pytest.raises(LoadingError):
                loader.load()
        finally:
            os.unlink(path)

    def test_with_pydantic_v2_validator(self):
        class Cfg(BaseModel):
            name: str

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp:
            tmp.write("name: hello\n")
            tmp.flush()
            path = tmp.name

        try:
            loader = YamlLoader(path, validator=PydanticV2Validator, schema=Cfg)
            data = loader.load()
            assert data == {"name": "hello"}
        finally:
            os.unlink(path)

    def test_custom_matcher(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp:
            tmp.write("key: ${MISSING}\n")
            tmp.flush()
            path = tmp.name

        try:
            loader = YamlLoader(path, matcher=StrictMatcher)
            data = loader.load()
            assert data == {"key": None}
        finally:
            os.unlink(path)
