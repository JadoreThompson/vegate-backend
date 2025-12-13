import requests


class HTTPSessMixin:
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self._http_sess = requests.Session()

    def _set_headers(self, headers: dict):
        self._http_sess.headers.update(headers)

    def _cleanup(self):
        self._http_sess.close()

    def __del__(self):
        self._cleanup()
