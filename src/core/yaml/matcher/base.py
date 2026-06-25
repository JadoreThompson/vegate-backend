import re

import yaml


class Matcher(yaml.SafeLoader):
    """
    Base class for matchers

    Use this class only to derive new child classes
    """

    matcher: re.Pattern[str] = re.compile("")

    @staticmethod
    def constructor(loader, node):
        raise NotImplementedError
