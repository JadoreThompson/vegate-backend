import os
import re

from .base import Matcher
from ..error import LoadingError


class MatcherWithDefaults(Matcher):
    """
    Expand environment variables with support for:
    - ${VAR}
    - ${VAR:-default}
    - ${VAR:?error message}
    """

    matcher = re.compile(r"\$\{([a-zA-Z_$0-9]+)(:([-\?]).*)?\}")

    @staticmethod
    def constructor(loader, node):
        match = MatcherWithDefaults.matcher.match(node.value)

        if not match:
            return node.value

        variable, raw, mode = match.groups()

        value = os.environ.get(variable)

        # Case 1: exists
        if value is not None:
            return value

        # Case 2: default fallback ${VAR:-default}
        if raw and raw.startswith(":-"):
            return raw[2:]

        # Case 3: required variable ${VAR:?error message}
        if raw and raw.startswith(":?"):
            message = raw[2:] or f"Missing required environment variable: {variable}"
            raise LoadingError(reason=message)

        # Case 4: plain missing variable
        return None
