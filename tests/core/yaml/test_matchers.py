import os
from unittest.mock import patch

import pytest
import yaml

from core.yaml.error import LoadingError
from core.yaml.matcher import Matcher, MatcherWithDefaults, StrictMatcher

# Register the !env resolver on the matcher classes so that
# ``yaml.load(..., Loader=…)`` triggers interpolation.
MatcherWithDefaults.add_implicit_resolver("!env", MatcherWithDefaults.matcher, None)
MatcherWithDefaults.add_constructor("!env", MatcherWithDefaults.constructor)
StrictMatcher.add_implicit_resolver("!env", StrictMatcher.matcher, None)
StrictMatcher.add_constructor("!env", StrictMatcher.constructor)


class TestMatcher:
    def test_constructor_raises_not_implemented(self):
        node = type("FakeNode", (), {"value": "${VAR}"})()
        with pytest.raises(NotImplementedError):
            Matcher.constructor(None, node)


class TestStrictMatcher:
    def test_resolves_existing_var(self):
        with patch.dict(os.environ, {"MY_VAR": "hello"}, clear=True):
            result = yaml.load("key: ${MY_VAR}", Loader=StrictMatcher)
        assert result == {"key": "hello"}

    def test_returns_none_for_missing_var(self):
        result = yaml.load("key: ${NOT_SET}", Loader=StrictMatcher)
        assert result == {"key": None}

    def test_resolves_multiple_vars(self):
        with patch.dict(os.environ, {"A": "1", "B": "2"}, clear=True):
            result = yaml.load("a: ${A}\nb: ${B}", Loader=StrictMatcher)
        assert result == {"a": "1", "b": "2"}


class TestMatcherWithDefaults:
    def test_resolves_existing_var(self):
        with patch.dict(os.environ, {"FOO": "bar"}, clear=True):
            result = yaml.load("key: ${FOO}", Loader=MatcherWithDefaults)
        assert result == {"key": "bar"}

    def test_default_fallback(self):
        result = yaml.load("key: ${MISSING:-fallback}", Loader=MatcherWithDefaults)
        assert result == {"key": "fallback"}

    def test_default_fallback_with_empty_default(self):
        result = yaml.load("key: ${MISSING:-}", Loader=MatcherWithDefaults)
        assert result == {"key": ""}

    def test_raises_loading_error_for_required_var(self):
        with pytest.raises(LoadingError, match="Missing required environment variable: REQUIRED"):
            yaml.load("key: ${REQUIRED:?}", Loader=MatcherWithDefaults)

    def test_raises_with_custom_message(self):
        with pytest.raises(LoadingError, match="Please set REQUIRED"):
            yaml.load("key: ${REQUIRED:?Please set REQUIRED}", Loader=MatcherWithDefaults)

    def test_returns_none_for_plain_missing(self):
        result = yaml.load("key: ${NOT_SET}", Loader=MatcherWithDefaults)
        assert result == {"key": None}

    def test_non_env_string_passes_through(self):
        result = yaml.load("key: hello", Loader=MatcherWithDefaults)
        assert result == {"key": "hello"}

    def test_all_patterns_in_one_doc(self):
        with patch.dict(os.environ, {"EXISTS": "present"}, clear=True):
            result = yaml.load(
                "\n".join(
                    [
                        "a: ${EXISTS}",
                        "b: ${MISSING:-default}",
                        "c: ${NOT_THERE}",
                    ]
                ),
                Loader=MatcherWithDefaults,
            )
        assert result == {"a": "present", "b": "default", "c": None}
