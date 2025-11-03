import logging
import os
import sys
from datetime import timedelta
from urllib.parse import quote

from dotenv import load_dotenv
from redis import Redis
from redis.asyncio import Redis as AsyncRedis
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine


BASE_PATH = os.path.dirname(__file__)
RESOURCES_PATH = os.path.join(BASE_PATH, "resources")

load_dotenv(os.path.join(BASE_PATH, ".env"))

IS_PRODUCTION = bool(os.getenv("IS_PRODUCTION"))

# Auth
COOKIE_ALIAS = "app-cookie"
JWT_ALGO = os.getenv("JWT_ALGO")
JWT_SECRET = os.getenv("JWT_SECRET")
JWT_EXPIRY = timedelta(seconds=int(os.getenv("JWT_EXPIRY_SECS")))

# DB
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_HOST_CREDS = f"{DB_HOST}:{DB_PORT}"
DB_USER_CREDS = f"{DB_USER}:{quote(DB_PASSWORD)}"
DB_ENGINE = create_async_engine(
    f"postgresql+asyncpg://{DB_USER_CREDS}@{DB_HOST_CREDS}/{DB_NAME}"
)
DB_ENGINE_SYNC = create_engine(
    f"postgresql+psycopg2://{DB_USER_CREDS}@{DB_HOST_CREDS}/{DB_NAME}"
)

# Redis
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
REDIS_DB = None
kw = {
    "host": REDIS_HOST,
    "port": REDIS_PORT,
    "db": REDIS_DB,
    "encoding": "utf-8",
    "decode_responses": True,
}
REDIS_CLIENT = AsyncRedis(**kw)
REDIS_CLIENT_SYNC = Redis(**kw)
del kw
REDIS_EMAIL_VERIFICATION_KEY_PREFIX = os.getenv("REDIS_EMAIL_VERIFICATION_KEY_PREFIX")
REDIS_STRIPE_INVOICE_METADATA_KEY_PREFIX = os.getenv(
    "REDIS_STRIPE_INVOICE_METADATA_KEY_PREFIX"
)
REDIS_USER_MODERATOR_MESSAGES_PREFIX = os.getenv("REDIS_USER_MODERATOR_MESSAGES_PREFIX")
REDIS_EXPIRY = 900

# Server
PAGE_SIZE = 10
if IS_PRODUCTION:
    SCHEME = "https"
    SUB_DOMAIN = "www."
    DOMAIN = "domain.com"
else:
    SCHEME = "http"
    SUB_DOMAIN = ""
    DOMAIN = "localhost:5173"

# Email
PERSONAL_EMAIL = os.getenv("PERSONAL_EMAIL")

# Security
PW_HASH_SALT = os.getenv("PW_HASH_SALT")
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
ENCRYPTION_IV_LEN = int(os.getenv("ENCRYPTION_IV_LEN"))

# Logging
logging.basicConfig(
    filename="app.log",
    filemode="a",
    format="%(asctime)s - [%(levelname)s] - %(module)s - %(message)s",
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(
    logging.Formatter("%(asctime)s - [%(levelname)s] - %(module)s - %(message)s")
)
logger.addHandler(handler)
del logger
