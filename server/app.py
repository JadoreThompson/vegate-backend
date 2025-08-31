import aiohttp
from fastapi import Depends, FastAPI

from server.dependencies import depends_http_sess


app = FastAPI()

