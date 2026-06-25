import os
import re
from .base import Matcher


class StrictMatcher(Matcher):
    """
    Expand an environment variable of form ${VAR} with its value

    If value is not set return None.
    """

    matcher = re.compile(r"\$\{([^}^{^:]+)\}")

    @staticmethod
    def constructor(loader, node):
        match = StrictMatcher.matcher.match(node.value)
        return os.environ.get(match.groups()[0])  # type: ignore
