from redis import ConnectionError as RedisConnectionError, Redis
from module.exception.retry import Retry

redis_retry = Retry(exceptions=[RedisConnectionError])


class RetryRedis:

    def __init__(self, redis_client: Redis):
        self.redis_client = redis_client

    def __getattribute__(self, name):
        if name == "redis_client":
            return object.__getattribute__(self, "redis_client")

        attr = getattr(self.redis_client, name)

        if not callable(attr):
            return attr

        return redis_retry(attr)
