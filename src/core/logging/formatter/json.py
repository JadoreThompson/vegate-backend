import json
import logging
from datetime import datetime, timezone

class JsonLogFormatter(logging.Formatter):

    def format(self, record):
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }

        return json.dumps(log_entry, default=str, ensure_ascii=False)
