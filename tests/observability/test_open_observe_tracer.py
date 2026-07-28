"""
``OpenObserveTracer`` — OTLP endpoint/header/resource construction.

``OTLPSpanExporter`` and ``BatchSpanProcessor`` are patched in every test, so
nothing performs DNS or network I/O and no exporter worker thread is ever
created (hence nothing to shut down). The global tracer provider is never
really set either: OpenTelemetry honors only the first ``set_tracer_provider``
per process, so letting a test through would make test order significant.
"""

import base64
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("opentelemetry.sdk", reason="requires the 'observability-tracing-core' extra")

from rococo.observability.tracing import open_observe_tracer as tracer_module  # noqa: E402
from rococo.observability.tracing.open_observe_tracer import OpenObserveTracer  # noqa: E402

BASE_URL = "http://oo.test:5080"
ORG = "myorg"
TOKEN = "mytoken"

EXPECTED_AUTH = "Basic " + base64.b64encode(f"{ORG}:{TOKEN}".encode()).decode()


@pytest.fixture
def patched_sdk():
    """Patches the exporter and the span processor — no threads, no network."""
    with patch.object(tracer_module, "OTLPSpanExporter") as exporter, \
            patch.object(tracer_module, "BatchSpanProcessor") as processor:
        yield exporter, processor


@pytest.fixture
def no_global_register():
    """Guards the process-global provider: set_tracer_provider is never real."""
    with patch.object(tracer_module.trace, "set_tracer_provider") as setter:
        yield setter


def build_provider(**overrides):
    kwargs = {
        "base_url": BASE_URL,
        "org_id": ORG,
        "ingestion_token": TOKEN,
        "service_name": "svc",
        "env": "test",
    }
    kwargs.update(overrides)
    # create_tracer_provider() never touches `self`, so it can be called
    # directly on the class without constructing (and registering) a tracer.
    return OpenObserveTracer.create_tracer_provider(None, **kwargs)


# --------------------------------------------------------------------------
# endpoint / headers
# --------------------------------------------------------------------------

@pytest.mark.parametrize("base_url", [BASE_URL, BASE_URL + "/"])
def test_endpoint_construction(patched_sdk, base_url):
    exporter, _ = patched_sdk

    build_provider(base_url=base_url)

    assert exporter.call_args.kwargs["endpoint"] == f"{BASE_URL}/api/{ORG}/v1/traces"


def test_headers_include_basic_auth_and_default_stream(patched_sdk):
    exporter, _ = patched_sdk

    build_provider()

    headers = exporter.call_args.kwargs["headers"]
    assert headers["Authorization"] == EXPECTED_AUTH
    assert headers["stream-name"] == "app-traces"


def test_headers_use_custom_stream(patched_sdk):
    exporter, _ = patched_sdk

    build_provider(stream="my-traces")

    assert exporter.call_args.kwargs["headers"]["stream-name"] == "my-traces"


# --------------------------------------------------------------------------
# resource attributes
# --------------------------------------------------------------------------

def test_resource_attributes_include_service_name_and_environment(patched_sdk):
    provider = build_provider(service_name="svc", env="staging")

    attributes = provider.resource.attributes
    assert attributes["service.name"] == "svc"
    assert attributes["deployment.environment"] == "staging"


@pytest.mark.parametrize("falsy_env", [None, ""])
def test_deployment_environment_omitted_when_env_is_falsy(patched_sdk, falsy_env):
    provider = build_provider(env=falsy_env)

    assert "deployment.environment" not in provider.resource.attributes


def test_service_name_none_is_coerced_by_the_sdk(patched_sdk):
    # Pinning actual behavior: Resource.create drops the invalid None value and
    # the SDK falls back to its own default service.name.
    provider = build_provider(service_name=None)

    assert "service.name" in provider.resource.attributes
    assert provider.resource.attributes["service.name"] is not None


# --------------------------------------------------------------------------
# provider wiring
# --------------------------------------------------------------------------

def test_batch_span_processor_added_to_provider(patched_sdk):
    exporter, processor = patched_sdk

    with patch.object(tracer_module, "TracerProvider") as provider_cls:
        build_provider()

    processor.assert_called_once_with(exporter.return_value)
    provider_cls.return_value.add_span_processor.assert_called_once_with(processor.return_value)


def test_create_tracer_provider_does_not_touch_the_global_provider(patched_sdk):
    from opentelemetry import trace

    before = trace.get_tracer_provider()

    build_provider()

    assert trace.get_tracer_provider() is before


# --------------------------------------------------------------------------
# registration
# --------------------------------------------------------------------------

def test_register_false_does_not_set_global_provider(patched_sdk, no_global_register):
    OpenObserveTracer(
        url=BASE_URL, ingestion_token=TOKEN, org_id=ORG, env="test", register=False
    )

    no_global_register.assert_not_called()


def test_register_true_sets_global_provider(patched_sdk, no_global_register):
    tracer = OpenObserveTracer(
        url=BASE_URL, ingestion_token=TOKEN, org_id=ORG, env="test", register=True
    )

    no_global_register.assert_called_once_with(tracer.provider)


def test_register_defaults_to_true(patched_sdk, no_global_register):
    tracer = OpenObserveTracer(url=BASE_URL, ingestion_token=TOKEN, org_id=ORG, env="test")

    no_global_register.assert_called_once_with(tracer.provider)


def test_register_tracer_provider_delegates_to_trace(patched_sdk, no_global_register):
    tracer = OpenObserveTracer(
        url=BASE_URL, ingestion_token=TOKEN, org_id=ORG, env="test", register=False
    )
    provider = MagicMock(name="other_provider")

    tracer.register_tracer_provider(provider)

    no_global_register.assert_called_once_with(provider)


def test_constructor_forwards_config_to_create_tracer_provider(patched_sdk, no_global_register):
    exporter, _ = patched_sdk

    OpenObserveTracer(
        url=BASE_URL + "/",
        ingestion_token=TOKEN,
        org_id=ORG,
        env="prod",
        stream="custom",
        service_name="svc",
        register=False,
    )

    kwargs = exporter.call_args.kwargs
    assert kwargs["endpoint"] == f"{BASE_URL}/api/{ORG}/v1/traces"
    assert kwargs["headers"]["stream-name"] == "custom"
