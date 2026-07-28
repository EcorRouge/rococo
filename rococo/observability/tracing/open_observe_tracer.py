import base64

from .._extras import raise_missing_extra

try:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
except ImportError as e:
    raise_missing_extra("observability-tracing-core", e)

# Re-exported for backwards compatibility: there used to be a second, subtly
# different `traced` here (it named spans with __name__ instead of __qualname__,
# so two same-named methods on different classes collided). There is now exactly
# one implementation, in decorators.py, which needs only opentelemetry-api.
from .decorators import traced  # noqa: E402,F401


class OpenObserveTracer:
    def __init__(self, url, ingestion_token, org_id, env, stream="app-traces", service_name=None, register=True):
        self.provider = self.create_tracer_provider(
            base_url=url,
            org_id=org_id,
            ingestion_token=ingestion_token,
            service_name=service_name,
            env=env,
            stream=stream,
        )
        if register:
            self.register_tracer_provider(self.provider)


    def create_tracer_provider(self, base_url, org_id, ingestion_token, service_name=None, env=None, stream="app-traces"):
        """
        Builds (but does not globally register) a TracerProvider exporting to
        OpenObserve via OTLP/HTTP. Caller decides whether/when to set it as the
        global provider — keeps this safe to call in tests without side effects.
        """
        auth_header = "Basic " + base64.b64encode((org_id + ':' + ingestion_token).encode()).decode()

        resource_attrs = {"service.name": service_name}
        if env:
            resource_attrs["deployment.environment"] = env

        exporter = OTLPSpanExporter(
            endpoint=f"{base_url.rstrip('/')}/api/{org_id}/v1/traces",
            headers={"Authorization": auth_header, "stream-name": stream},
        )

        provider = TracerProvider(resource=Resource.create(resource_attrs))
        provider.add_span_processor(BatchSpanProcessor(exporter))
        return provider


    def register_tracer_provider(self, provider):
        """Sets the given provider as the global default for this process."""
        trace.set_tracer_provider(provider)
