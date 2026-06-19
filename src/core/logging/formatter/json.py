import json
import logging
from datetime import datetime, timezone

from opentelemetry import trace

class JsonLogFormatter(logging.Formatter):

    def format(self, record):
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }

        try:
            span = trace.get_current_span()
            if span and span.get_span_context().is_valid:
                log_entry["trace_id"] = format(span.get_span_context().trace_id, "032x")
                log_entry["span_id"] = format(span.get_span_context().span_id, "016x")
        except:
            pass

        return json.dumps(log_entry, default=str, ensure_ascii=False)
