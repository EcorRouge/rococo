
from .base import ObservabilityBase
from .logging.open_observe_handler import OpenObserveHandler


class OpenObserve(ObservabilityBase):
    """
    OpenObserve integration for logging and tracing.

    Required config kwargs:
        OO_BASE_URL (str): Base URL of the OpenObserve instance, e.g. "http://host:5080".
        OO_ORG_ID (str): Organization identifier (the slug, not the display name).
        OO_INGESTION_TOKEN (str): Ingestion token for this org, from the Ingestion page.

    Optional config kwargs:
        SERVICE_NAME (str): Used to tag logs/traces with `service`/`service.name`.
            If unset, tracing resource attributes and log tagging are skipped.
        APP_ENV (str): Environment label (dev/staging/production), tagged on
            logs and traces. Defaults to no `env` field if unset.
    """

    REQUIRED_CONFIG_KEYS = ("OO_BASE_URL", "OO_ORG_ID", "OO_INGESTION_TOKEN")

    def setup(self):
        # config
        self.oo_base_url = self.config.get("OO_BASE_URL")
        self.oo_org_id = self.config.get("OO_ORG_ID")
        self.oo_ingestion_token = self.config.get("OO_INGESTION_TOKEN")
        self.service_name = self.config.get("SERVICE_NAME")
        if not all([self.oo_base_url, self.oo_org_id, self.oo_ingestion_token]):
            raise ValueError(
                "OpenObserve configuration is incomplete. Please set OO_BASE_URL, OO_ORG_ID, and OO_INGESTION_TOKEN."
            )
        
        # state
        self._requests_instrumented = False
        self._psycopg2_instrumented = False
        self._httpx_instrumented = False
        self._langgraph_instrumented = False
        self._tracer_provider_registered = False


    def _get_stream(self, stream_name):
        return f"{stream_name}"

    def get_logging_handler(self, env=None, stream=None):
        return OpenObserveHandler(
            base_url=self.oo_base_url,
            org_id=self.oo_org_id,
            ingestion_token=self.oo_ingestion_token,
            env=env or self.config.get("APP_ENV"),
            stream=stream or self._get_stream('logs'),
            service_name=self.service_name,
        )

    def get_tracer_provider(self, env=None, service_name=None, stream=None, register=True):
        # OpenObserveTracer itself needs opentelemetry-sdk + otlp exporter —
        # these ARE core requirements for any service doing tracing at all,
        # so this import can stay at module level in open_observe.py/open_observe_tracer.py
        from .tracing.open_observe_tracer import OpenObserveTracer

        tracer = OpenObserveTracer(
            url=self.oo_base_url,
            org_id=self.oo_org_id,
            ingestion_token=self.oo_ingestion_token,
            service_name=service_name or self.service_name,
            env=env or self.config.get("APP_ENV"),
            stream=stream or self._get_stream("traces"),
            register=register and not self._tracer_provider_registered,
        )
        if register:
            self._tracer_provider_registered = True
        return tracer.provider

    def enable_flask_tracing(self, app, excluded_urls="^/$,^/health$"):
        """
        One-call setup for Flask apps: registers the tracer provider (if not
        already registered), instruments Flask + requests, and wraps every
        view function in its own span — including streaming/SSE views, where
        the span stays open for the full duration of response iteration
        (not just the view function's initial return), so persistence logic
        that runs during/after streaming still nests correctly.
 
        App code only ever needs to call this single method.
        """
        try:
            from opentelemetry.instrumentation.flask import FlaskInstrumentor
            from opentelemetry.instrumentation.requests import RequestsInstrumentor
        except ImportError as e:
            from ._extras import raise_missing_extra
            raise_missing_extra("observability-flask-tracing", e)
 
        self.get_tracer_provider()   # no-ops registration if already done
 
        FlaskInstrumentor().instrument_app(
            app,
            excluded_urls=excluded_urls   # regex — excludes exact root path "/" and "/health"
        )
        if not self._requests_instrumented:
            RequestsInstrumentor().instrument(
                excluded_urls=self.oo_base_url  # regex-matched — excludes calls to OpenObserve itself
            )
            self._requests_instrumented = True
 
        self._wrap_view_functions(app)
 
    def _wrap_view_functions(self, app, excluded_endpoints=None):
        """
        Wraps every registered Flask view function in its own span, named
        after the Python function. Must run AFTER all blueprints/routes are
        registered, since app.view_functions is only fully populated by then.
        """
        from opentelemetry import trace
        excluded_endpoints = excluded_endpoints or set()
        tracer = trace.get_tracer(__name__)
 
        for endpoint, view_func in app.view_functions.items():
            if endpoint in excluded_endpoints:
                continue
            app.view_functions[endpoint] = self._traced_view(tracer, view_func)
 
    def _traced_view(self, tracer, view_func):
        """
        Wraps a single view function. Detects Werkzeug streaming responses
        (SSE, chunked, etc.) and keeps the span open across the full body
        iteration rather than closing it the instant the view returns the
        Response object — otherwise any work that happens while the stream
        is being consumed (e.g. persistence at the end of an SSE generator)
        runs with no active span/parent context at all.
        """
        from opentelemetry import trace, context as otel_context
        from functools import wraps
        try:
            from werkzeug.wrappers import Response
        except ImportError as e:
            from ._extras import raise_missing_extra
            raise_missing_extra("observability-flask-tracing", e)
 
        span_name = f"{view_func.__module__}.{view_func.__name__}"
 
        @wraps(view_func)
        def wrapper(*args, **kwargs):
            span = tracer.start_span(span_name)
            ctx = trace.set_span_in_context(span)
            token = otel_context.attach(ctx)
 
            try:
                result = view_func(*args, **kwargs)
            except Exception as e:
                span.record_exception(e)
                span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                otel_context.detach(token)
                span.end()
                raise

            if isinstance(result, Response) and result.is_streamed:
                # Real work may still be happening (streaming body, and any
                # persistence logic run at the end of the generator) — keep
                # the span alive across that, not just the view's return.
                otel_context.detach(token)
                result.response = self._traced_stream(tracer, span, ctx, result.response)
                return result
 
            # Normal, non-streaming view — span covers exactly this call
            otel_context.detach(token)
            span.end()
            return result
 
        return wrapper
 
    def _traced_stream(self, tracer, span, ctx, body_iter):
        from opentelemetry import trace, context as otel_context

        it = iter(body_iter)
        try:
            while True:
                token = otel_context.attach(ctx)
                try:
                    chunk = next(it)   # the real work happens HERE — now correctly inside the attached context
                finally:
                    otel_context.detach(token)
                yield chunk
        except StopIteration:
            return
        except Exception as e:
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
            raise
        finally:
            # Client disconnect (GeneratorExit) closes THIS generator at its
            # last `yield` — but that does NOT automatically close `it`.
            # Close it explicitly, with context attached, so any cleanup logic
            # in stream_chat_message (e.g. _persist_interrupted_stream) also
            # runs under the correct span.
            if hasattr(it, "close"):
                token = otel_context.attach(ctx)
                try:
                    it.close()
                finally:
                    otel_context.detach(token)
            span.end()

    def enable_fastapi_tracing(self, app, excluded_urls="^/$,^/health$"):
        """
        One-call setup for FastAPI apps: registers the tracer provider (if
        not already registered) and instruments FastAPI + httpx. App code
        only ever needs to call this single method, once, at app startup.

        excluded_urls: comma-separated regex patterns for paths to skip
        (defaults to root "/" and "/health" — typical health-check noise).
        """

        # Lazy imports — only services that call this need
        # opentelemetry-instrumentation-fastapi/-httpx installed
        try:
            from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
            from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
        except ImportError as e:
            from ._extras import raise_missing_extra
            raise_missing_extra("observability-fastapi-tracing", e)

        self.get_tracer_provider()   # no-ops registration if already done

        FastAPIInstrumentor.instrument_app(app, excluded_urls=excluded_urls)

        if not self._httpx_instrumented:
            HTTPXClientInstrumentor().instrument()
            self._httpx_instrumented = True


    def enable_postgres_tracing(self):
        """
        Patches psycopg2 globally so every query run through it produces a
        span automatically — no changes needed in adapter/repository code.

        Call this exactly once per process, as early as possible, before any
        psycopg2 connection is created (e.g. from RepositoryFactory.__init__,
        guarded by a class-level flag + lock — see the calling pattern used
        there). Safe to call more than once; only instruments the first time.
        """
        # Lazy import — only services that call this need
        # opentelemetry-instrumentation-psycopg2 installed
        try:
            from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
        except ImportError as e:
            from ._extras import raise_missing_extra
            raise_missing_extra("observability-postgres-tracing", e)

        if not self._psycopg2_instrumented:
            Psycopg2Instrumentor().instrument(
                enable_commenter=True,   # tags SQL with trace context as a
                                         # comment — lets you correlate a slow
                                         # query in Postgres logs/pg_stat_statements
                                         # back to the exact trace that issued it
                commenter_options={},
            )
            self._psycopg2_instrumented = True


    def enable_langgraph_tracing(self):
        """
        Instruments LangChain/LangGraph via OpenInference — hooks into
        langchain-core, the shared foundation underlying LangGraph, so this
        covers node/edge execution, chain/tool calls, and LLM invocations
        automatically, with zero changes needed in chatbot.py.

        Spans are emitted through whatever tracer provider is already
        globally registered — call get_tracer_provider() / enable_fastapi_tracing()
        / enable_flask_tracing() first, so LangGraph spans nest correctly
        under the active request span rather than being orphaned.

        Safe to call multiple times; only instruments once per process.
        """

        # Lazy import — only services that call this need
        # openinference-instrumentation-langchain installed
        try:
            from openinference.instrumentation.langchain import LangChainInstrumentor
        except ImportError as e:
            from ._extras import raise_missing_extra
            raise_missing_extra("observability-langgraph-tracing", e)

        if not self._langgraph_instrumented:
            LangChainInstrumentor().instrument()
            self._langgraph_instrumented = True


    def enable_class_tracing(self, cls, skip_methods=None, name_prefix=None, include_subclasses=True):
        """
        Wraps every public, callable, non-dunder method defined directly on
        `cls` with the shared @traced decorator. If include_subclasses=True
        (default), also recursively wraps every currently-loaded subclass's
        own directly-defined methods — so calling this once on the top of
        your repository hierarchy covers the whole tree.

            self.enable_class_tracing(PostgreSQLRepository)
            # traces PostgreSQLRepository, BaseRepository, and every
            # concrete repo (DataImportJobRepository, etc.) in one call

        Note: only classes already imported at call time are visible via
        __subclasses__() — call this after your views/modules that define
        repository subclasses have been imported (e.g. after
        initialize_views(api) in create_app()).
        """
        # decorators.traced needs only opentelemetry-api (the base extra) —
        # class tracing emits spans on whatever provider is already registered,
        # so it must not drag in the SDK via open_observe_tracer.
        from .tracing.decorators import traced
        import inspect

        default_skip = {"__init__", "__init_subclass__"}
        skip_methods = default_skip | set(skip_methods or [])

        self._trace_single_class(cls, skip_methods, name_prefix, traced, inspect)

        if include_subclasses:
            for subclass in self._all_subclasses(cls):
                self._trace_single_class(subclass, skip_methods, name_prefix, traced, inspect)

    def _trace_single_class(self, cls, skip_methods, name_prefix, traced, inspect):
        if cls.__dict__.get("_oo_class_traced", False):
            return
        for name, attr in list(vars(cls).items()):
            if name.startswith("_") or name in skip_methods:
                continue
            if inspect.isfunction(attr):
                span_name = f"{name_prefix or cls.__name__}.{name}"
                setattr(cls, name, traced(name=span_name)(attr))
        cls._oo_class_traced = True

    def _all_subclasses(self, cls):
        direct = set(cls.__subclasses__())
        return direct.union(s for sub in direct for s in self._all_subclasses(sub))