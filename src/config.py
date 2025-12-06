import logging
import os
import sys
from urllib.parse import quote

import stripe
from dotenv import load_dotenv


# Paths
BASE_PATH = os.path.dirname(__file__)
PARENT_PATH = os.path.dirname(BASE_PATH)
RESOURCES_PATH = os.path.join(BASE_PATH, "resources")

load_dotenv(os.path.join(PARENT_PATH, ".env"))

IS_PRODUCTION = bool(os.getenv("IS_PRODUCTION"))

# Auth
COOKIE_ALIAS = "app-cookie"
JWT_ALGO = os.getenv("JWT_ALGO")
JWT_SECRET = os.getenv("JWT_SECRET")
JWT_EXPIRY_SECS = int(os.getenv("JWT_EXPIRY_SECS"))

# DB
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = quote(os.getenv("DB_PASSWORD"))
DB_NAME = os.getenv("DB_NAME")
DB_HOST_CREDS = f"{DB_HOST}:{DB_PORT}"
DB_USER_CREDS = f"{DB_USER}:{DB_PASSWORD}"

# Kafka
KAFKA_HOST = os.getenv("KAFKA_HOST")
KAFKA_PORT = int(os.getenv("KAFKA_PORT"))

# Redis
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
REDIS_USERNAME = os.getenv("REDIS_USERNAME")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_DB = 0
REDIS_EMAIL_VERIFICATION_KEY_PREFIX = os.getenv("REDIS_EMAIL_VERIFICATION_KEY_PREFIX")
REDIS_STRIPE_INVOICE_METADATA_KEY_PREFIX = os.getenv(
    "REDIS_STRIPE_INVOICE_METADATA_KEY_PREFIX"
)
REDIS_EXPIRY_SECS = 900

# Server
PAGE_SIZE = 10
if IS_PRODUCTION:
    SCHEME = "https"
    SUB_DOMAIN = "api."
    DOMAIN = "domain.com"
else:
    SCHEME = "http"
    SUB_DOMAIN = ""
    DOMAIN = "localhost:5173"

# Email
CUSTOMER_SUPPORT_EMAIL = os.getenv("CUSTOMER_SUPPORT_EMAIL")

# Security
PW_HASH_SALT = os.getenv("PW_HASH_SALT")
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
ENCRYPTION_IV_LEN = int(os.getenv("ENCRYPTION_IV_LEN"))

# Stripe
STRIPE_API_KEY = os.getenv("STRIPE_API_KEY")
STRIPE_PRICING_PRO_WEBHOOOK_SECRET = os.getenv("STRIPE_PRICING_PRO_WEBHOOOK_SECRET")
STRIPE_PRICING_PRO_PRICE_ID = os.getenv("STRIPE_PRICING_PRO_PRICE_ID")
stripe.api_key = STRIPE_API_KEY

# Alpaca
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
ALPACA_BASE_URL = "https://data.alpaca.markets/v2/stocks/{symbol}/trades"

# Logging
logging.basicConfig(
    filename="app.log",
    filemode="a",
    format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s",
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(
    logging.Formatter("%(asctime)s - [%(levelname)s] - %(module)s - %(message)s")
)
logger.addHandler(handler)
del logger

kafka_logger = logging.getLogger("kafka")
kafka_logger.setLevel(logging.CRITICAL)
del kafka_logger

aiokafka_logger = logging.getLogger("aiokafka")
aiokafka_logger.setLevel(logging.CRITICAL)
del aiokafka_logger

stripe_logger = logging.getLogger("stripe")
stripe_logger.setLevel(logging.CRITICAL)
del stripe_logger
