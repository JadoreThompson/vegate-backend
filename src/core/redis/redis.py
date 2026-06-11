from redis import Redis, ConnectionError
from redis.asyncio import Redis as AsyncRedis

from module.exception.retry import Retry


redis_retry = Retry(exceptions={ConnectionError})


class CustomRedis(Redis):

    @redis_retry
    def execute_command(self, *args, **options):
        return super().execute_command(*args, **options)


class AsyncCustomRedis(AsyncRedis):

    @redis_retry
    def execute_command(self, *args, **options):
        return super().execute_command(*args, **options)
