import logging
import os
import sys
import yaml
from urllib.parse import quote

from dotenv import load_dotenv


SRC_PATH = os.path.dirname(__file__)
PROJECT_PATH = os.path.dirname(SRC_PATH)

PYTEST_RUNNING = bool(os.getenv("PYTEST_VERSION"))
load_dotenv(os.path.join(PROJECT_PATH, ".env.test" if PYTEST_RUNNING else ".env"))

IS_PRODUCTION = bool(int(os.getenv("IS_PRODUCTION", "0")))

with open(os.path.join(PROJECT_PATH, "feed.yaml"), "r") as f:
    CONFIG_YAML = yaml.load(f, Loader=yaml.SafeLoader)


# Server
SCHEME = os.getenv("SCHEME", "http")
FRONTEND_SUB_DOMAIN = os.getenv("FRONTEND_SUB_DOMAIN", "")
FRONTEND_DOMAIN = os.getenv("FRONTEND_DOMAIN", "localhost:5173")


# JWT
COOKIE_ALIAS = "vegate-cookie"
JWT_ALGO = os.getenv("JWT_ALGO")
JWT_SECRET = os.getenv("JWT_SECRET")
JWT_EXPIRY_SECS = int(os.getenv("JWT_EXPIRY_SECS"))


# Security
PW_HASH_SALT = os.getenv("PW_HASH_SALT")
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
ENCRYPTION_IV_LEN = int(os.getenv("ENCRYPTION_IV_LEN"))

STRATEGY_DEPLOYMENT_EVENTS_KEY = os.getenv(
    "STRATEGY_DEPLOYMENT_EVENTS_KEY", "strategy_deployment_events"
)

BACKTEST_EVENTS_KEY = os.getenv(
    "BACKTEST_EVENTS_KEY", "backtest_events"
)


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
REDIS_EMAIL_VERIFCATION_EXPIRY_SECS = int(os.getenv("REDIS_EMAIL_VERIFCATION_EXPIRY_SECS", "900"))
REDIS_CHANGE_EMAIL_KEY_PREFIX = "change_email:"

REDIS_PASSWORD_RESET_TOKEN_KEY_PREFIX = os.getenv("REDIS_PASSWORD_RESET_TOKEN_KEY_PREFIX", "password_reset:token:")
REDIS_PASSWORD_RESET_EXPIRY_SECS = int(os.getenv("REDIS_PASSWORD_RESET_EXPIRY_SECS", "900"))
REDIS_CHANGE_PASSWORD_KEY_PREFIX = os.getenv("REDIS_CHANGE_PASSWORD_KEY_PREFIX", "change_password:")

REDIS_CHANGE_USERNAME_KEY_PREFIX = os.getenv("REDIS_CHANGE_USERNAME_KEY_PREFIX", "change_username:")

VERIFICATION_CODE_EXPIRY_SECS = int(os.getenv("VERIFICATION_CODE_EXPIRY_SECS", "300"))

REDIS_ALPACA_OAUTH_PREFIX = os.getenv("REDIS_ALPACA_OAUTH_PREFIX")
REDIS_ALPACA_OAUTH_TTL_SECS = int(os.getenv("REDIS_ALPACA_OAUTH_TTL_SECS"))

REDIS_STRATEGY_DEPLOYMENT_HEARTBEAT_KEY_PREFIX = os.getenv(
    "REDIS_STRATEGY_HEARTBEAT_KEY_PREFIX", "strategy_deployment:heartbeat:"
)
REDIS_BACKTEST_HEARTBEAT_KEY_PREFIX = os.getenv(
    "REDIS_BACKTEST_HEARTBEAT_KEY_PREFIX", "backtest:heartbeat:"
)


# Kafka
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = int(os.getenv("KAFKA_PORT", "9092"))
KAFKA_BOOTSTRAP_SERVERS = f"{KAFKA_HOST}:{KAFKA_PORT}"


# Docker
IMAGE_NAME = os.getenv("IMAGE_NAME", "vegate-backend:latest")


# OHLC
OHLC_FEED_HOST = os.getenv("OHLC_FEED_HOST", "localhost")
OHLC_FEED_PORT = int(os.getenv("OHLC_FEED_PORT", "8001"))


# OMS
OMS_BASE_URL = os.getenv("OMS_BASE_URL", "http://localhost:8082")
OMS_SESSION_PREFIX = os.getenv("OMS_SESSION_PREFIX", "oms:session:")


# Historical Data
HISTORICAL_BASE_URL = os.getenv("HISTORICAL_BASE_URL", "http://localhost:8000")


# Backtest
MAX_CONCURRENT_BACKTESTS = int(os.getenv("MAX_CONCURRENT_BACKTESTS", "5"))
BACKTEST_EXECUTOR_NAME = os.getenv("BACKTEST_EXECUTOR_NAME", "process")


# Deployment
MAX_CONCURRENT_DEPLOYMENTS = int(os.getenv("MAX_CONCURRENT_BACKTESTS", "5"))
DEPLOYMENT_EXECUTOR_NAME = os.getenv("DEPLOYMENT_EXECUTOR_NAME", "process")


# Email
CUSTOMER_SUPPORT_EMAIL = os.getenv("CUSTOMER_SUPPORT_EMAIL")
BREVO_API_KEY = os.getenv("BREVO_API_KEY")
SMTPGO_API_KEY = os.getenv("SMTPGO_API_KEY")


# Alpaca
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
ALPACA_OAUTH_CLIENT_ID = os.getenv("ALPACA_OAUTH_CLIENT_ID")
ALPACA_OAUTH_SECRET_KEY = os.getenv("ALPACA_OAUTH_SECRET_KEY")
ALPACA_OAUTH_REDIRECT_URI = os.getenv("ALPACA_OAUTH_REDIRECT_URI")


# LLM
LLM_API_KEY = os.getenv("LLM_API_KEY", "api-key")
LLM_MODEL_NAME = os.getenv("LLM_MODEL_NAME", "mistral-small-latest")


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
