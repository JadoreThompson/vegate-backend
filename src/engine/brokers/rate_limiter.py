import threading
from time import time, sleep


class TokenBucketRateLimiter:
    """
    Token bucket rate limiter for broker API calls.

    Implements the token bucket algorithm to control the rate of API requests.
    Tokens are refilled at a constant rate, and each request consumes one token.
    If no tokens are available, the request waits until a token becomes available.

    This prevents exceeding broker API rate limits while allowing bursts of
    requests up to the bucket capacity.

    Attributes:
        rate: Maximum number of requests allowed per period
        per_seconds: Time period in seconds for the rate limit
        allowance: Current number of tokens available
        last_check: Timestamp of last token consumption

    Example:
        # Allow 200 requests per 60 seconds
        limiter = TokenBucketRateLimiter(rate=200, per_seconds=60)

        def make_request():
            limiter.acquire()  # Wait for permission
            # Make API request here
    """

    def __init__(self, rate: int, per_seconds: int = 60):
        """
        Initialize rate limiter.

        Args:
            rate: Maximum number of requests allowed per period
            per_seconds: Time period in seconds (default: 60)

        Example:
            # 100 requests per minute
            limiter = TokenBucketRateLimiter(rate=100, per_seconds=60)

            # 10 requests per second
            limiter = TokenBucketRateLimiter(rate=10, per_seconds=1)
        """
        self.rate = rate
        self.per_seconds = per_seconds
        self.allowance = float(rate)
        self.last_check = time()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """
        Acquire permission to make a request.

        This method blocks until a token is available. Tokens are refilled
        continuously at the configured rate. If the bucket is full, tokens
        are not accumulated beyond the maximum rate.

        This method is thread-safe through the use of a lock.

        Example:
            def call_api():
                limiter.acquire()  # Wait for permission
                response = broker.get_account()
                return response
        """
        with self._lock:
            current = time()
            time_passed = current - self.last_check
            self.last_check = current

            # Refill tokens based on time passed
            self.allowance += time_passed * (self.rate / self.per_seconds)

            # Cap at maximum rate (bucket size)
            if self.allowance > self.rate:
                self.allowance = self.rate

            # If insufficient tokens, wait for refill
            if self.allowance < 1.0:
                sleep_time = (1.0 - self.allowance) * (self.per_seconds / self.rate)
                sleep(sleep_time)
                self.allowance = 0.0
            else:
                self.allowance -= 1.0

    def get_current_allowance(self) -> float:
        """
        Get the current number of available tokens (non-blocking).

        This is useful for monitoring rate limiter state without
        consuming a token.

        Returns:
            Current number of tokens available
        """
        current = time()
        time_passed = current - self.last_check
        allowance = self.allowance + time_passed * (self.rate / self.per_seconds)
        return min(allowance, self.rate)

    def reset(self) -> None:
        """
        Reset the rate limiter to full capacity.

        This is useful for testing or when switching between different
        rate limit contexts.
        """
        with self._lock:
            self.allowance = float(self.rate)
            self.last_check = time()
