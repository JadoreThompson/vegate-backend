import json
import logging
import requests
from datetime import datetime, timezone
from threading import Thread


class LokiLogHandler(logging.Handler):
    def __init__(self, loki_url, labels):
        super().__init__()
        self.loki_url = loki_url
        self.labels = labels

    def emit(self, record):
        log_entry = self.format(record)
        stream = {
            **self.labels,
            "level": record.levelname.lower(),
            "logger": record.name,
        }
        payload = {
            "streams": [
                {
                    "stream": stream,
                    "values": [
                        [
                            str(int(datetime.now(timezone.utc).timestamp() * 1e9)),
                            log_entry,
                        ]
                    ],
                }
            ]
        }

        Thread(target=self._send_log, args=(payload,)).start()

    def _send_log(self, payload):
        try:
            requests.post(
                f"{self.loki_url}/loki/api/v1/push",
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                timeout=2,
            )
        except Exception as e:
            print(f"Failed to push log to Loki: {e}")
