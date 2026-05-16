from enums import BrokerType


class BrokerAccountFetchException(Exception):
    pass


class UnsupportedBrokerException(Exception):

    def __init__(self, broker_type: BrokerType):
        super().__init__(f"Unsupported broker '{broker_type}'")
