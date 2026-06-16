from typing import IO, Any, Type, Union

import yaml

from .base import YamlLoader
from ..error import LoadingError
from ..matcher import Matcher, MatcherWithDefaults
from ..validator import Validator


class YamlStreamLoader(YamlLoader):
    """
    YAML configuration loader for IO streams, e.g. file objects or stdin
    """

    def __init__(
        self,
        stream: Union[str, IO[str]],
        *,
        matcher: Type[Matcher] = MatcherWithDefaults,
        validator: Union[Type[Validator], None] = None,
        schema: Any | None = None,
        **schema_params,
    ) -> None:
        self.stream = stream
        self.matcher = matcher
        self.validator = validator
        self.schema = schema
        self.schema_params = schema_params

    def load(self, **params) -> Any:
        self._init_resolvers()
        try:
            load = yaml.load(self.stream, Loader=self.matcher)
        except yaml.YAMLError as e:
            raise LoadingError(origin=e, reason=str(e))

        if (self.validator is not None) and (self.schema is not None):
            return self.validator(self.schema, **self.schema_params).load(
                data=load, **params
            )
        return load
