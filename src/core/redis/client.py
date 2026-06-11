from redis import Redis
from redis.asyncio import Redis as AsyncRedis

from config import REDIS_HOST, REDIS_PORT, REDIS_USERNAME, REDIS_PASSWORD, REDIS_DB
from .ext.retry import RetryRedis

kw = {
    "host": REDIS_HOST,
    "port": REDIS_PORT,
    "username": REDIS_USERNAME,
    "password": REDIS_PASSWORD,
    "db": REDIS_DB,
}
# REDIS_CLIENT = AsyncRedis(**kw)
# REDIS_CLIENT_SYNC = Redis(**kw)
REDIS_CLIENT = RetryRedis(redis_client=AsyncRedis(**kw))
REDIS_CLIENT_SYNC = RetryRedis(redis_client=Redis(**kw))

del kw
