from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult


class SimpleConsoleExporter(SpanExporter):
    def export(self, spans):
        for span in spans:
            print({"trace_id": format(span.context.trace_id, "032x")})
        return SpanExportResult.SUCCESS
