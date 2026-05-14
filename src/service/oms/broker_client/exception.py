class BrokerClientException(Exception):
    # pass
    def __init__(self, message: str):
        super().__init__(f"Broker client exception: {message}")
