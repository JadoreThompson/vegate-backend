class OMSClientException(Exception):
    pass


class OMSClientRetryExhausted(Exception):
    """Raised when retries are exhausted and caller should fallback to sync handling."""
    pass


class FuturesOMSClientException(Exception):
    pass


class FuturesOMSClientRetryExhausted(Exception):
    pass
