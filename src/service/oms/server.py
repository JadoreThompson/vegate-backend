from fastapi import FastAPI

from service.oms.service import OMSService

app = FastAPI()


class OMSServer:

    def __init__(self, oms_service: OMSService, uvicorn_kw: dict):
        self._oms_service: OMSService = oms_service
        self._uvicorn_kw = uvicorn_kw

    