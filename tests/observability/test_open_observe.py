"""
The ``OpenObserve`` provider.

Everything here is hermetic: the instrumentor packages, ``werkzeug`` and the
SDK-dependent tracer module are injected as stubs via the ``stub_module``
fixture, so these tests behave identically whether or not opentelemetry happens
to be installed. That matters most for the register-once / instrument-once
semantics, which are pure state machines and shouldn't depend on the venv.
"""

import types
from unittest.mock import MagicMock, patch

import pytest

from rococo.observability.open_observe import OpenObserve

REQUIRED_KEYS = ("OO_BASE_URL", "OO_ORG_ID", "OO_INGESTION_TOKEN")
OPTIONAL_KEYS = ("SERVICE_NAME", "APP_ENV")


# --------------------------------------------------------------------------
# fakes
# --------------------------------------------------------------------------

class FakeSpan:
    def __init__(self, name):
        self.name = name
        self.ended = 0
        self.recorded_exceptions = []
        self.statuses = []

    def end(self):
        self.ended += 1

    def record_exception(self, exc):
        self.recorded_exceptions.append(exc)

    def set_status(self, status):
        self.statuses.append(status)


class FakeTracer:
    def __init__(self):
        self.spans = []

    def start_span(self, name):
        span = FakeSpan(name)
        self.spans.append(span)
        return span


class FakeStatus:
    def __init__(self, code, description=None):
        self.code = code
        self.description = description


class FakeStatusCode:
    ERROR = "ERROR"
    OK = "OK"


class FakeResponse:
    """Stands in for werkzeug.wrappers.Response."""

    def __init__(self, response=None, is_streamed=False):
        self.response = response
        self.is_streamed = is_streamed


@pytest.fixture
def fake_otel(stub_module):
    """
    Installs a minimal fake ``opentelemetry`` (trace + context) and
    ``werkzeug.wrappers``, and returns a recorder for attach/detach tokens.
    """
    recorder = types.SimpleNamespace(attached=[], detached=[], tracers=[])

    def get_tracer(name):
        tracer = FakeTracer()
        tracer.instrumentation_name = name
        recorder.tracers.append(tracer)
        return tracer

    def attach(ctx):
        token = f"token-{len(recorder.attached)}"
        recorder.attached.append((token, ctx))
        return token

    def detach(token):
        recorder.detached.append(token)

    trace_module = stub_module(
        "opentelemetry.trace",
        get_tracer=get_tracer,
        set_span_in_context=lambda span: ("ctx", span),
        Status=FakeStatus,
        StatusCode=FakeStatusCode,
        get_current_span=MagicMock(),
    )
    context_module = stub_module("opentelemetry.context", attach=attach, detach=detach)
    stub_module("werkzeug.wrappers", Response=FakeResponse)

    recorder.trace = trace_module
    recorder.context = context_module
    return recorder


@pytest.fixture
def fake_tracer_module(stub_module):
    """
    Replaces ``rococo.observability.tracing.open_observe_tracer`` with a stub
    exposing a recording ``OpenObserveTracer`` (and a recording ``traced``),
    so no opentelemetry SDK is needed to test the provider's wiring.
    """
    calls = []

    class RecordingOpenObserveTracer:
        def __init__(self, **kwargs):
            calls.append(kwargs)
            self.provider = MagicMock(name="tracer_provider")

    traced_calls = []

    def traced(name=None):
        def decorator(func):
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            wrapper._traced_name = name
            wrapper._wrapped = func
            traced_calls.append(name)
            return wrapper

        return decorator

    stub_module(
        "rococo.observability.tracing.open_observe_tracer",
        OpenObserveTracer=RecordingOpenObserveTracer,
        traced=traced,
    )
    # enable_class_tracing() pulls `traced` from the API-only decorators module,
    # so that is the one it must see stubbed.
    stub_module("rococo.observability.tracing.decorators", traced=traced)
    return types.SimpleNamespace(calls=calls, traced_calls=traced_calls)


@pytest.fixture
def provider(valid_config):
    return OpenObserve(**valid_config)


# --------------------------------------------------------------------------
# construction / config
# --------------------------------------------------------------------------

def test_required_config_keys_are_exactly_the_three_connection_keys():
    # Only the connection triple is required; SERVICE_NAME/APP_ENV are optional,
    # matching the class docstring and setup()'s own validation.
    assert OpenObserve.REQUIRED_CONFIG_KEYS == REQUIRED_KEYS


def test_construction_sets_attributes_from_config(provider, valid_config):
    assert provider.oo_base_url == valid_config["OO_BASE_URL"]
    assert provider.oo_org_id == valid_config["OO_ORG_ID"]
    assert provider.oo_ingestion_token == valid_config["OO_INGESTION_TOKEN"]
    assert provider.service_name == valid_config["SERVICE_NAME"]
    assert provider.config == valid_config


@pytest.mark.parametrize(
    "flag",
    [
        "_requests_instrumented",
        "_psycopg2_instrumented",
        "_httpx_instrumented",
        "_langgraph_instrumented",
        "_tracer_provider_registered",
    ],
)
def test_state_flags_start_false(provider, flag):
    assert getattr(provider, flag) is False


@pytest.mark.parametrize("omitted", REQUIRED_KEYS)
def test_omitting_any_required_key_raises_value_error(valid_config, omitted):
    config = {k: v for k, v in valid_config.items() if k != omitted}

    with pytest.raises(ValueError) as excinfo:
        OpenObserve(**config)

    assert omitted in str(excinfo.value)


@pytest.mark.parametrize("omitted", OPTIONAL_KEYS)
def test_omitting_an_optional_key_is_allowed(valid_config, omitted):
    config = {k: v for k, v in valid_config.items() if k != omitted}

    provider = OpenObserve(**config)  # must not raise

    assert provider.config == config


def test_construction_with_only_the_connection_triple(valid_config):
    config = {k: valid_config[k] for k in REQUIRED_KEYS}

    provider = OpenObserve(**config)

    assert provider.service_name is None
    assert provider.config.get("APP_ENV") is None


def test_optional_keys_degrade_gracefully_in_logging_handler(valid_config):
    config = {k: valid_config[k] for k in REQUIRED_KEYS}
    provider = OpenObserve(**config)

    with patch("rococo.observability.open_observe.OpenObserveHandler") as handler_cls:
        provider.get_logging_handler()

    kwargs = handler_cls.call_args.kwargs
    assert kwargs["service_name"] is None  # handler omits the `service` field
    assert kwargs["env"] is None


def test_optional_keys_degrade_gracefully_in_tracer_provider(valid_config, fake_tracer_module):
    config = {k: valid_config[k] for k in REQUIRED_KEYS}
    provider = OpenObserve(**config)

    provider.get_tracer_provider()

    kwargs = fake_tracer_module.calls[0]
    assert kwargs["service_name"] is None
    assert kwargs["env"] is None  # tracer omits `deployment.environment`


@pytest.mark.parametrize("stream_name", ["logs", "traces", "anything"])
def test_get_stream_is_identity(provider, stream_name):
    # Identity today; pinned so adding an org/env prefix is a deliberate change.
    assert provider._get_stream(stream_name) == stream_name


# --------------------------------------------------------------------------
# get_logging_handler
# --------------------------------------------------------------------------

def test_get_logging_handler_wiring(provider, valid_config):
    with patch("rococo.observability.open_observe.OpenObserveHandler") as handler_cls:
        result = provider.get_logging_handler()

    handler_cls.assert_called_once_with(
        base_url=valid_config["OO_BASE_URL"],
        org_id=valid_config["OO_ORG_ID"],
        ingestion_token=valid_config["OO_INGESTION_TOKEN"],
        env=valid_config["APP_ENV"],
        stream="logs",
        service_name=valid_config["SERVICE_NAME"],
    )
    assert result is handler_cls.return_value


def test_get_logging_handler_explicit_overrides(provider):
    with patch("rococo.observability.open_observe.OpenObserveHandler") as handler_cls:
        provider.get_logging_handler(env="staging", stream="mystream")

    kwargs = handler_cls.call_args.kwargs
    assert kwargs["env"] == "staging"
    assert kwargs["stream"] == "mystream"


@pytest.mark.parametrize("falsy", ["", None, 0])
def test_get_logging_handler_falsy_override_falls_back_to_config(provider, valid_config, falsy):
    # `env or self.config.get("APP_ENV")` — an empty override is not honored.
    with patch("rococo.observability.open_observe.OpenObserveHandler") as handler_cls:
        provider.get_logging_handler(env=falsy, stream=falsy)

    kwargs = handler_cls.call_args.kwargs
    assert kwargs["env"] == valid_config["APP_ENV"]
    assert kwargs["stream"] == "logs"


# --------------------------------------------------------------------------
# get_tracer_provider
# --------------------------------------------------------------------------

def test_get_tracer_provider_returns_provider(provider, fake_tracer_module):
    result = provider.get_tracer_provider()

    assert result is not None
    assert len(fake_tracer_module.calls) == 1


def test_get_tracer_provider_forwards_config_defaults(provider, fake_tracer_module, valid_config):
    provider.get_tracer_provider()

    kwargs = fake_tracer_module.calls[0]
    assert kwargs == {
        "url": valid_config["OO_BASE_URL"],
        "org_id": valid_config["OO_ORG_ID"],
        "ingestion_token": valid_config["OO_INGESTION_TOKEN"],
        "service_name": valid_config["SERVICE_NAME"],
        "env": valid_config["APP_ENV"],
        "stream": "traces",
        "register": True,
    }


def test_get_tracer_provider_forwards_overrides(provider, fake_tracer_module):
    provider.get_tracer_provider(env="prod", service_name="other", stream="custom-traces")

    kwargs = fake_tracer_module.calls[0]
    assert kwargs["env"] == "prod"
    assert kwargs["service_name"] == "other"
    assert kwargs["stream"] == "custom-traces"


def test_get_tracer_provider_registers_once(provider, fake_tracer_module):
    first = provider.get_tracer_provider()
    assert provider._tracer_provider_registered is True

    second = provider.get_tracer_provider()

    assert fake_tracer_module.calls[0]["register"] is True
    assert fake_tracer_module.calls[1]["register"] is False
    assert first is not None and second is not None


def test_get_tracer_provider_register_false_leaves_flag_unset(provider, fake_tracer_module):
    provider.get_tracer_provider(register=False)

    assert fake_tracer_module.calls[0]["register"] is False
    assert provider._tracer_provider_registered is False


# --------------------------------------------------------------------------
# enable_* missing extras
# --------------------------------------------------------------------------

def assert_missing_extra(excinfo, extra):
    message = str(excinfo.value)
    assert extra in message
    assert f"pip install 'rococo[{extra}]'" in message
    assert isinstance(excinfo.value.__cause__, ImportError)


def test_enable_flask_tracing_missing_extra(provider, block_modules):
    # reset=() keeps rococo.observability itself imported: only the lazy
    # instrumentation import inside enable_flask_tracing() may fail here.
    block_modules("opentelemetry.instrumentation", "werkzeug", "flask", reset=())

    with pytest.raises(ImportError) as excinfo:
        provider.enable_flask_tracing(app=MagicMock())

    assert_missing_extra(excinfo, "observability-flask-tracing")
    assert provider._requests_instrumented is False


def test_enable_fastapi_tracing_missing_extra(provider, block_modules):
    block_modules("opentelemetry.instrumentation", reset=())

    with pytest.raises(ImportError) as excinfo:
        provider.enable_fastapi_tracing(app=MagicMock())

    assert_missing_extra(excinfo, "observability-fastapi-tracing")
    assert provider._httpx_instrumented is False


def test_enable_postgres_tracing_missing_extra(provider, block_modules):
    block_modules("opentelemetry.instrumentation", reset=())

    with pytest.raises(ImportError) as excinfo:
        provider.enable_postgres_tracing()

    assert_missing_extra(excinfo, "observability-postgres-tracing")
    assert provider._psycopg2_instrumented is False


def test_enable_langgraph_tracing_missing_extra(provider, block_modules):
    block_modules("openinference", reset=())

    with pytest.raises(ImportError) as excinfo:
        provider.enable_langgraph_tracing()

    assert_missing_extra(excinfo, "observability-langgraph-tracing")
    assert provider._langgraph_instrumented is False


# --------------------------------------------------------------------------
# enable_* happy paths
# --------------------------------------------------------------------------

@pytest.fixture
def psycopg2_instrumentor(stub_module):
    instrumentor = MagicMock(name="Psycopg2Instrumentor")
    stub_module("opentelemetry.instrumentation.psycopg2", Psycopg2Instrumentor=instrumentor)
    return instrumentor


def test_enable_postgres_tracing_instruments_once(provider, psycopg2_instrumentor):
    provider.enable_postgres_tracing()
    provider.enable_postgres_tracing()

    psycopg2_instrumentor.return_value.instrument.assert_called_once_with(
        enable_commenter=True, commenter_options={}
    )
    assert provider._psycopg2_instrumented is True


@pytest.fixture
def langchain_instrumentor(stub_module):
    instrumentor = MagicMock(name="LangChainInstrumentor")
    stub_module("openinference.instrumentation.langchain", LangChainInstrumentor=instrumentor)
    return instrumentor


def test_enable_langgraph_tracing_instruments_once(provider, langchain_instrumentor):
    provider.enable_langgraph_tracing()
    provider.enable_langgraph_tracing()

    langchain_instrumentor.return_value.instrument.assert_called_once_with()
    assert provider._langgraph_instrumented is True


@pytest.fixture
def fastapi_instrumentors(stub_module):
    fastapi_instrumentor = MagicMock(name="FastAPIInstrumentor")
    httpx_instrumentor = MagicMock(name="HTTPXClientInstrumentor")
    stub_module("opentelemetry.instrumentation.fastapi", FastAPIInstrumentor=fastapi_instrumentor)
    stub_module("opentelemetry.instrumentation.httpx", HTTPXClientInstrumentor=httpx_instrumentor)
    return types.SimpleNamespace(fastapi=fastapi_instrumentor, httpx=httpx_instrumentor)


def test_enable_fastapi_tracing_instruments_app(provider, fastapi_instrumentors, fake_tracer_module):
    app = MagicMock(name="app")

    provider.enable_fastapi_tracing(app)

    fastapi_instrumentors.fastapi.instrument_app.assert_called_once_with(
        app, excluded_urls="^/$,^/health$"
    )
    fastapi_instrumentors.httpx.return_value.instrument.assert_called_once_with()
    assert provider._httpx_instrumented is True
    assert len(fake_tracer_module.calls) == 1, "tracer provider must be registered"


def test_enable_fastapi_tracing_forwards_custom_excluded_urls(
    provider, fastapi_instrumentors, fake_tracer_module
):
    app = MagicMock(name="app")

    provider.enable_fastapi_tracing(app, excluded_urls="^/metrics$")

    assert fastapi_instrumentors.fastapi.instrument_app.call_args.kwargs["excluded_urls"] == "^/metrics$"


def test_enable_fastapi_tracing_instruments_httpx_only_once(
    provider, fastapi_instrumentors, fake_tracer_module
):
    provider.enable_fastapi_tracing(MagicMock())
    provider.enable_fastapi_tracing(MagicMock())

    fastapi_instrumentors.httpx.return_value.instrument.assert_called_once_with()


@pytest.fixture
def flask_instrumentors(stub_module):
    flask_instrumentor = MagicMock(name="FlaskInstrumentor")
    requests_instrumentor = MagicMock(name="RequestsInstrumentor")
    stub_module("opentelemetry.instrumentation.flask", FlaskInstrumentor=flask_instrumentor)
    stub_module("opentelemetry.instrumentation.requests", RequestsInstrumentor=requests_instrumentor)
    return types.SimpleNamespace(flask=flask_instrumentor, requests=requests_instrumentor)


@pytest.fixture
def flask_app():
    app = MagicMock(name="app")
    app.view_functions = {"index": _index_view, "health": _health_view}
    return app


def _index_view():
    return "index"


def _health_view():
    return "ok"


def test_enable_flask_tracing_instruments_app_and_wraps_views(
    provider, flask_app, flask_instrumentors, fake_otel, fake_tracer_module, valid_config
):
    originals = dict(flask_app.view_functions)

    provider.enable_flask_tracing(flask_app)

    flask_instrumentors.flask.return_value.instrument_app.assert_called_once_with(
        flask_app, excluded_urls="^/$,^/health$"
    )
    flask_instrumentors.requests.return_value.instrument.assert_called_once_with(
        excluded_urls=valid_config["OO_BASE_URL"]
    )
    assert provider._requests_instrumented is True
    for endpoint, original in originals.items():
        assert flask_app.view_functions[endpoint] is not original
        assert callable(flask_app.view_functions[endpoint])


def test_enable_flask_tracing_instruments_requests_only_once(
    provider, flask_app, flask_instrumentors, fake_otel, fake_tracer_module
):
    provider.enable_flask_tracing(flask_app)
    provider.enable_flask_tracing(flask_app)

    flask_instrumentors.requests.return_value.instrument.assert_called_once()


def test_wrap_view_functions_honors_excluded_endpoints(provider, flask_app, fake_otel):
    original = flask_app.view_functions["health"]

    provider._wrap_view_functions(flask_app, excluded_endpoints={"health"})

    assert flask_app.view_functions["health"] is original
    assert flask_app.view_functions["index"] is not _index_view


# --------------------------------------------------------------------------
# _traced_view / _traced_stream
# --------------------------------------------------------------------------

def test_traced_view_non_streaming(provider, fake_otel):
    tracer = FakeTracer()

    wrapper = provider._traced_view(tracer, _index_view)
    result = wrapper()

    assert result == "index"
    span = tracer.spans[0]
    assert span.name == f"{_index_view.__module__}.{_index_view.__name__}"
    assert span.ended == 1
    assert fake_otel.detached == ["token-0"]


def test_traced_view_preserves_functools_wraps(provider, fake_otel):
    wrapper = provider._traced_view(FakeTracer(), _index_view)

    assert wrapper.__name__ == _index_view.__name__
    assert wrapper.__wrapped__ is _index_view


def test_traced_view_forwards_args_and_kwargs(provider, fake_otel):
    def view(a, b=None):
        return (a, b)

    wrapper = provider._traced_view(FakeTracer(), view)

    assert wrapper(1, b=2) == (1, 2)


def test_traced_view_records_and_reraises_exception(provider, fake_otel):
    boom = RuntimeError("boom")

    def view():
        raise boom

    tracer = FakeTracer()
    wrapper = provider._traced_view(tracer, view)

    with pytest.raises(RuntimeError) as excinfo:
        wrapper()

    assert excinfo.value is boom
    span = tracer.spans[0]
    assert span.recorded_exceptions == [boom]
    assert span.statuses[0].code == FakeStatusCode.ERROR
    assert span.ended == 1
    assert fake_otel.detached == ["token-0"]


def test_traced_view_streaming_keeps_span_open_until_consumed(provider, fake_otel):
    chunks = ["a", "b", "c"]

    def view():
        return FakeResponse(response=iter(chunks), is_streamed=True)

    tracer = FakeTracer()
    wrapper = provider._traced_view(tracer, view)

    response = wrapper()
    span = tracer.spans[0]

    # The core fix: the span is NOT ended when the view returns.
    assert span.ended == 0
    assert response.response is not chunks

    assert list(response.response) == chunks
    assert span.ended == 1


def test_traced_view_streaming_attaches_context_per_chunk(provider, fake_otel):
    def view():
        return FakeResponse(response=iter(["a", "b"]), is_streamed=True)

    response = provider._traced_view(FakeTracer(), view)()
    list(response.response)

    # one attach for the view call itself, then one per next() (including the
    # final StopIteration probe) plus one for the explicit close.
    assert len(fake_otel.attached) >= 4
    assert len(fake_otel.detached) == len(fake_otel.attached)


def test_traced_stream_records_exception_raised_mid_iteration(provider, fake_otel):
    boom = ValueError("stream broke")

    def body():
        yield "a"
        raise boom

    tracer = FakeTracer()
    span = tracer.start_span("test")

    generator = provider._traced_stream(tracer, span, ("ctx", span), body())

    assert next(generator) == "a"
    with pytest.raises(ValueError) as excinfo:
        next(generator)

    assert excinfo.value is boom
    assert span.recorded_exceptions == [boom]
    assert span.statuses[0].code == FakeStatusCode.ERROR
    assert span.ended == 1


def test_traced_stream_early_close_closes_underlying_iterator(provider, fake_otel):
    closed = []

    class Body:
        def __iter__(self):
            return self

        def __next__(self):
            return "chunk"

        def close(self):
            closed.append(True)

    tracer = FakeTracer()
    span = tracer.start_span("test")

    generator = provider._traced_stream(tracer, span, ("ctx", span), Body())
    assert next(generator) == "chunk"

    generator.close()  # simulates a client disconnect

    assert closed == [True]
    assert span.ended == 1


def test_traced_stream_ends_span_on_normal_exhaustion(provider, fake_otel):
    tracer = FakeTracer()
    span = tracer.start_span("test")

    generator = provider._traced_stream(tracer, span, ("ctx", span), iter([]))

    assert list(generator) == []
    assert span.ended == 1


def test_traced_view_non_streamed_response_object_ends_span(provider, fake_otel):
    response = FakeResponse(response=["static"], is_streamed=False)

    tracer = FakeTracer()
    wrapper = provider._traced_view(tracer, lambda: response)
    result = wrapper()

    assert result is response
    assert response.response == ["static"]
    assert tracer.spans[0].ended == 1


def test_traced_view_missing_werkzeug_raises_flask_extra(provider, fake_otel, block_modules):
    # Only the werkzeug import is guarded here; the `from opentelemetry import
    # trace, context` above it is unguarded because opentelemetry-api ships in
    # the base extra, so the fake otel stays installed while werkzeug is hidden.
    block_modules("werkzeug", reset=())

    with pytest.raises(ImportError) as excinfo:
        provider._traced_view(FakeTracer(), _index_view)

    assert_missing_extra(excinfo, "observability-flask-tracing")


# --------------------------------------------------------------------------
# enable_class_tracing
# --------------------------------------------------------------------------

def make_target_class():
    class Target:
        CONSTANT = 42

        def public(self):
            return "public"

        def other(self):
            return "other"

        def _private(self):
            return "private"

        def __dunder__(self):  # pragma: no cover - never called
            return "dunder"

        def __init__(self):
            self.built = True

        @staticmethod
        def a_static():
            return "static"

        @property
        def a_property(self):
            return "property"

    return Target


def traced_name_of(func):
    return getattr(func, "_traced_name", None)


def test_enable_class_tracing_wraps_public_methods(provider, fake_tracer_module):
    Target = make_target_class()
    original_public = Target.public

    provider.enable_class_tracing(Target)

    assert Target.public is not original_public
    assert traced_name_of(Target.public) == "Target.public"
    assert traced_name_of(Target.other) == "Target.other"
    assert Target().public() == "public"


def test_enable_class_tracing_skips_private_dunder_and_init(provider, fake_tracer_module):
    Target = make_target_class()
    originals = {name: vars(Target)[name] for name in ("_private", "__dunder__", "__init__")}

    provider.enable_class_tracing(Target)

    for name, original in originals.items():
        assert vars(Target)[name] is original


def test_enable_class_tracing_honors_skip_methods(provider, fake_tracer_module):
    Target = make_target_class()
    original = Target.other

    provider.enable_class_tracing(Target, skip_methods=["other"])

    assert Target.other is original
    assert Target.public is not None
    assert traced_name_of(Target.public) == "Target.public"


def test_enable_class_tracing_leaves_non_functions_alone(provider, fake_tracer_module):
    Target = make_target_class()
    static_original = vars(Target)["a_static"]
    property_original = vars(Target)["a_property"]

    provider.enable_class_tracing(Target)

    # inspect.isfunction() is False for staticmethod objects and properties,
    # so both survive untouched.
    assert vars(Target)["a_static"] is static_original
    assert vars(Target)["a_property"] is property_original
    assert Target.CONSTANT == 42


def test_enable_class_tracing_name_prefix(provider, fake_tracer_module):
    Target = make_target_class()

    provider.enable_class_tracing(Target, name_prefix="X")

    assert traced_name_of(Target.public) == "X.public"


def test_enable_class_tracing_wraps_subclasses_by_default(provider, fake_tracer_module):
    Base = make_target_class()

    class Middle(Base):
        def middle_method(self):
            return "middle"

    class Leaf(Middle):
        def leaf_method(self):
            return "leaf"

    provider.enable_class_tracing(Base)

    assert traced_name_of(Middle.middle_method) == "Middle.middle_method"
    assert traced_name_of(Leaf.leaf_method) == "Leaf.leaf_method"


def test_enable_class_tracing_can_skip_subclasses(provider, fake_tracer_module):
    Base = make_target_class()

    class Sub(Base):
        def sub_method(self):
            return "sub"

    original = Sub.sub_method

    provider.enable_class_tracing(Base, include_subclasses=False)

    assert Sub.sub_method is original
    assert traced_name_of(Base.public) == "Target.public"


def test_enable_class_tracing_is_idempotent(provider, fake_tracer_module):
    Target = make_target_class()

    provider.enable_class_tracing(Target)
    wrapped_once = Target.public
    provider.enable_class_tracing(Target)

    assert Target.public is wrapped_once
    assert Target.__dict__["_oo_class_traced"] is True


def test_subclass_defined_after_tracing_is_not_considered_traced(provider, fake_tracer_module):
    Base = make_target_class()
    provider.enable_class_tracing(Base)

    class Late(Base):
        def late_method(self):
            return "late"

    # The guard flag is inherited as an attribute but checked via cls.__dict__,
    # so a fresh subclass is still wrappable.
    assert Late._oo_class_traced is True
    assert Late.__dict__.get("_oo_class_traced") is None

    provider.enable_class_tracing(Late)

    assert traced_name_of(Late.late_method) == "Late.late_method"


def test_all_subclasses_returns_transitive_subclasses(provider):
    class A:
        pass

    class B(A):
        pass

    class C(B):
        pass

    class D(A):
        pass

    assert provider._all_subclasses(A) == {B, C, D}
    assert provider._all_subclasses(C) == set()


def test_enable_class_tracing_needs_only_the_base_extra(provider, block_modules):
    # Class tracing pulls `traced` from decorators.py (opentelemetry-api only),
    # so a service that just wants spans on the already-registered provider does
    # NOT need the SDK: blocking the SDK/exporter alone must not break it.
    block_modules(
        "opentelemetry.sdk",
        "opentelemetry.exporter",
        reset=("rococo.observability.tracing",),
    )

    provider.enable_class_tracing(make_target_class())  # must not raise


def test_enable_class_tracing_missing_base_extra(provider, block_modules):
    block_modules("opentelemetry", reset=("rococo.observability.tracing",))

    with pytest.raises(ImportError) as excinfo:
        provider.enable_class_tracing(make_target_class())

    assert_missing_extra(excinfo, "observability")
