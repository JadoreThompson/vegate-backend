import logging
import os
import sys
from urllib.parse import quote

import stripe
from dotenv import load_dotenv


# Paths
BASE_PATH = os.path.dirname(__file__)
PROJECT_PATH = os.path.dirname(BASE_PATH)
RESOURCES_PATH = os.path.join(BASE_PATH, "resources")

PYTEST_RUNNING = bool(os.getenv("PYTEST_VERSION"))

load_dotenv(os.path.join(PROJECT_PATH, ".env.test" if PYTEST_RUNNING else ".env"))

IS_PRODUCTION = bool(os.getenv("IS_PRODUCTION"))

# Auth
COOKIE_ALIAS = "vegate-cookie"
JWT_ALGO = os.getenv("JWT_ALGO")
JWT_SECRET = os.getenv("JWT_SECRET")
JWT_EXPIRY_SECS = int(os.getenv("JWT_EXPIRY_SECS"))

# Server
PAGE_SIZE = 10
if IS_PRODUCTION:
    SCHEME = "https"
    FRONTEND_SUB_DOMAIN = "www."
    BACKEND_SUB_DOMAIN = "api."
    FRONTEND_DOMAIN = "domain.com"
    BACKEND_DOMAIN = FRONTEND_DOMAIN
else:
    SCHEME = "http"
    FRONTEND_SUB_DOMAIN = ""
    BACKEND_SUB_DOMAIN = ""
    FRONTEND_DOMAIN = "localhost:5173"
    BACKEND_DOMAIN = "localhost:8000"

# Security
PW_HASH_SALT = os.getenv("PW_HASH_SALT")
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
ENCRYPTION_IV_LEN = int(os.getenv("ENCRYPTION_IV_LEN"))
BARS_WS_TOKEN = "your-token"

# DB
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT"))
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = quote(os.getenv("DB_PASSWORD"))
DB_NAME = os.getenv("DB_NAME")
DB_HOST_CREDS = f"{DB_HOST}:{DB_PORT}"
DB_USER_CREDS = f"{DB_USERNAME}:{DB_PASSWORD}"

# Redis
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
REDIS_USERNAME = os.getenv("REDIS_USERNAME")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
# Keys
REDIS_EMAIL_VERIFICATION_KEY_PREFIX = os.getenv("REDIS_EMAIL_VERIFICATION_KEY_PREFIX")
REDIS_EMAIL_VERIFCATION_EXPIRY_SECS = 900
REDIS_STRIPE_INVOICE_METADATA_KEY_PREFIX = os.getenv(
    "REDIS_STRIPE_INVOICE_METADATA_KEY_PREFIX"
)
REDIS_ALPACA_OAUTH_PREFIX = os.getenv("REDIS_ALPACA_OAUTH_PREFIX")
REDIS_ALPACA_OAUTH_TTL_SECS = int(os.getenv("REDIS_ALPACA_OAUTH_TTL_SECS"))
REDIS_DEPLOYMENT_EVENTS_KEY = os.getenv("REDIS_DEPLOYMENT_EVENTS_KEY")
REDIS_ORDER_EVENTS_KEY = os.getenv("REDIS_ORDER_EVENTS_KEY")
REDIS_BROKER_TRADE_EVENTS_KEY = os.getenv("REDIS_BROKER_TRADE_EVENTS_KEY")
REDIS_CANDLE_CLOSE_EVENTS_KEY = os.getenv("REDIS_CANDLE_CLOSE_EVENTS_KEY")
REDIS_SNAPSHOT_EVENTS_KEY = os.getenv("REDIS_SNAPSHOT_EVENTS_KEY")

# Kafka
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = int(os.getenv("KAFKA_PORT", "9092"))
KAFKA_BOOTSTRAP_SERVERS = f"{KAFKA_HOST}:{KAFKA_PORT}"

# LLM
LLM_API_KEY = os.getenv("LLM_API_KEY")

# Email
CUSTOMER_SUPPORT_EMAIL = os.getenv("CUSTOMER_SUPPORT_EMAIL")
BREVO_API_KEY = os.getenv("BREVO_API_KEY")

# Stripe
STRIPE_API_KEY = os.getenv("STRIPE_API_KEY")
STRIPE_PRICING_PRO_WEBHOOOK_SECRET = os.getenv("STRIPE_PRICING_PRO_WEBHOOOK_SECRET")
STRIPE_PRICING_PRO_PRICE_ID = os.getenv("STRIPE_PRICING_PRO_PRICE_ID")
stripe.api_key = STRIPE_API_KEY

# Alpaca
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
ALPACA_OAUTH_CLIENT_ID = os.getenv("ALPACA_OAUTH_CLIENT_ID")
ALPACA_OAUTH_SECRET_KEY = os.getenv("ALPACA_OAUTH_SECRET_KEY")
ALPACA_OAUTH_REDIRECT_URI = "http://localhost:8000/brokers/alpaca/oauth/callback"
ALPACA_OAUTH_ACCESS_TOKEN = os.getenv("ALPACA_OAUTH_ACCESS_TOKEN")

# Railway
RAILWAY_API_KEY = os.getenv("RAILWAY_API_KEY")
RAILWAY_PROJECT_ID = os.getenv("RAILWAY_PROJECT_ID")
RAILWAY_SERVICE_IMAGE = os.getenv("RAILWAY_SERVICE_IMAGE")
RAILWAY_ENVIRONMENT_ID = os.getenv("RAILWAY_ENVIRONMENT_ID")

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
    logging.Formatter("%(asctime)s - [%(levelname)s] - %(name)s - %(message)s")
)
logger.addHandler(handler)
del logger

stripe_logger = logging.getLogger("stripe")
stripe_logger.setLevel(logging.CRITICAL)
del stripe_logger
