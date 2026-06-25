import io
import os
from unittest.mock import patch

import pytest
from pydantic import BaseModel

from core.yaml import YamlStreamLoader
from core.yaml.error import LoadingError
from core.yaml.validator import PydanticV2Validator


class TestYamlStreamLoader:

    def test_loads_simple_list(self):
        stream = io.StringIO("- one\n- two\n- three\n")
        assert YamlStreamLoader(stream).load() == ["one", "two", "three"]

    def test_loads_list_of_dicts(self):
        stream = io.StringIO("- name: a\n  port: 1\n- name: b\n  port: 2\n")
        assert YamlStreamLoader(stream).load() == [
            {"name": "a", "port": 1},
            {"name": "b", "port": 2},
        ]

    def test_loads_from_string_stream(self):
        stream = io.StringIO("key: value\n")
        loader = YamlStreamLoader(stream)
        data = loader.load()
        assert data == {"key": "value"}

    def test_interpolates_env_var(self):
        stream = io.StringIO("key: ${STREAM_VAR}\n")
        with patch.dict(os.environ, {"STREAM_VAR": "stream_val"}, clear=True):
            loader = YamlStreamLoader(stream)
            data = loader.load()
        assert data == {"key": "stream_val"}

    def test_raises_loading_error_on_invalid_yaml(self):
        stream = io.StringIO(": broken [[\n")
        loader = YamlStreamLoader(stream)
        with pytest.raises(LoadingError):
            loader.load()

    def test_with_pydantic_v2_validator(self):
        class Cfg(BaseModel):
            port: int

        stream = io.StringIO("port: 8080\n")
        loader = YamlStreamLoader(stream, validator=PydanticV2Validator, schema=Cfg)
        data = loader.load()
        assert data == {"port": 8080}
