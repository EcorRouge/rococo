"""
Import-time behavior of rococo.observability.

Two things are pinned here:

1. ``import rococo.observability`` needs exactly the base ``observability``
   extra — ``requests`` and ``opentelemetry-api``, nothing heavier. Neither the
   SDK, the exporter, nor any framework instrumentation may be reachable from
   the base import path, and when the base extra itself is missing the failure
   must name that extra rather than leaking a bare ``No module named ...``.
2. Every guarded optional import must fail with an ``ImportError`` naming the
   exact extra to install, chained from the original error, rather than a bare
   ``No module named ...``.
"""

import importlib

import pytest

from rococo.observability._extras import raise_missing_extra


def import_fresh(name):
    return importlib.import_module(name)


# --------------------------------------------------------------------------
# the base import needs exactly the base extra
# --------------------------------------------------------------------------

BASE_IMPORT_ONLY_NEEDS = ("opentelemetry", "requests")

# Everything the base import path must NOT reach for: the SDK, the exporter and
# every framework instrumentation package. A stray top-level import of any of
# these would break consumers who installed only `rococo[observability]`.
NON_BASE_MODULES = (
    "opentelemetry.sdk",
    "opentelemetry.exporter",
    "opentelemetry.instrumentation",
    "openinference",
    "werkzeug",
    "flask",
    "fastapi",
    "psycopg2",
    "httpx",
)


def test_base_import_needs_nothing_beyond_the_base_extra(block_modules):
    block_modules(*NON_BASE_MODULES)

    module = import_fresh("rococo.observability")

    assert hasattr(module, "OpenObserve")
    assert hasattr(module, "PROVIDERS")
    assert hasattr(module, "get_observability_provider")


def test_open_observe_module_needs_nothing_beyond_the_base_extra(block_modules):
    block_modules(*NON_BASE_MODULES)

    module = import_fresh("rococo.observability.open_observe")

    assert module.OpenObserve.REQUIRED_CONFIG_KEYS


def test_base_module_imports_with_everything_optional_blocked(block_modules):
    block_modules(*NON_BASE_MODULES)

    assert import_fresh("rococo.observability.base").ObservabilityBase


@pytest.mark.parametrize(
    "name",
    ["rococo.observability.logging", "rococo.observability.tracing"],
)
def test_namespace_packages_import_with_optional_extras_blocked(block_modules, name):
    block_modules(*NON_BASE_MODULES)

    assert import_fresh(name) is not None


@pytest.mark.parametrize("missing", BASE_IMPORT_ONLY_NEEDS)
def test_base_import_without_the_base_extra_names_that_extra(block_modules, missing):
    # The base import legitimately requires `requests` and `opentelemetry-api`
    # (both ship in the `observability` extra) — the logging handler needs the
    # former to ship logs and the latter to correlate them with the active
    # trace. What must never happen is a bare ModuleNotFoundError.
    block_modules(missing)

    with pytest.raises(ImportError) as excinfo:
        import_fresh("rococo.observability")

    assert_missing_extra_error(excinfo, "observability")


# --------------------------------------------------------------------------
# provider registry
# --------------------------------------------------------------------------

def test_providers_registry_contents():
    from rococo.observability import PROVIDERS
    from rococo.observability.open_observe import OpenObserve

    assert PROVIDERS == {"open_observe": OpenObserve}


def test_get_observability_provider_returns_class():
    from rococo.observability import OpenObserve, get_observability_provider

    assert get_observability_provider("open_observe") is OpenObserve


def test_get_observability_provider_unknown_name():
    from rococo.observability import get_observability_provider

    with pytest.raises(ValueError) as excinfo:
        get_observability_provider("datadog")

    message = str(excinfo.value)
    assert "datadog" in message
    assert "Unknown observability provider" in message
    assert "open_observe" in message


@pytest.mark.parametrize("bad_name", [None, 123, ("open_observe",)])
def test_get_observability_provider_non_string_raises_value_error(bad_name):
    from rococo.observability import get_observability_provider

    # The `except KeyError` path covers unhashable-free non-strings too, so the
    # caller only ever has to handle ValueError.
    with pytest.raises(ValueError):
        get_observability_provider(bad_name)


# --------------------------------------------------------------------------
# missing-extra errors
# --------------------------------------------------------------------------

def assert_missing_extra_error(excinfo, extra):
    message = str(excinfo.value)
    assert extra in message
    assert f"pip install 'rococo[{extra}]'" in message
    assert isinstance(excinfo.value.__cause__, ImportError)


def test_decorators_import_without_opentelemetry_names_base_extra(block_modules):
    # reset only the tracing subpackage: the failure must come from decorators.py
    # itself, not from re-importing the parent package's logging handler.
    block_modules("opentelemetry", reset=("rococo.observability.tracing",))

    with pytest.raises(ImportError) as excinfo:
        import_fresh("rococo.observability.tracing.decorators")

    assert_missing_extra_error(excinfo, "observability")


def test_tracer_import_without_opentelemetry_names_tracing_core(block_modules):
    block_modules("opentelemetry", reset=("rococo.observability.tracing",))

    with pytest.raises(ImportError) as excinfo:
        import_fresh("rococo.observability.tracing.open_observe_tracer")

    assert_missing_extra_error(excinfo, "observability-tracing-core")


def test_tracer_import_without_sdk_only_names_tracing_core(block_modules):
    # The realistic case: opentelemetry-api is installed (base extra), but the
    # SDK/exporter are not — the extra named must still be the tracing core one.
    block_modules(
        "opentelemetry.sdk",
        "opentelemetry.exporter",
        reset=("rococo.observability.tracing",),
    )

    with pytest.raises(ImportError) as excinfo:
        import_fresh("rococo.observability.tracing.open_observe_tracer")

    assert_missing_extra_error(excinfo, "observability-tracing-core")


def test_handler_import_without_requests_names_base_extra(block_modules):
    block_modules("requests")

    with pytest.raises(ImportError) as excinfo:
        import_fresh("rococo.observability.logging.open_observe_handler")

    assert_missing_extra_error(excinfo, "observability")


def test_handler_import_without_opentelemetry_names_base_extra(block_modules):
    # The API is a hard requirement of the handler (every record is trace
    # correlated), so this fails at import time rather than inside emit().
    block_modules("opentelemetry", reset=("rococo.observability.logging",))

    with pytest.raises(ImportError) as excinfo:
        import_fresh("rococo.observability.logging.open_observe_handler")

    assert_missing_extra_error(excinfo, "observability")


# --------------------------------------------------------------------------
# raise_missing_extra itself
# --------------------------------------------------------------------------

def test_raise_missing_extra_raises_import_error():
    original = ImportError("No module named 'flask'")

    with pytest.raises(ImportError) as excinfo:
        raise_missing_extra("observability-flask-tracing", original)

    message = str(excinfo.value)
    assert "observability-flask-tracing" in message
    assert "pip install 'rococo[observability-flask-tracing]'" in message
    assert "No module named 'flask'" in message
    assert "rococo.observability" in message


def test_raise_missing_extra_chains_original_error():
    original = ImportError("boom")

    with pytest.raises(ImportError) as excinfo:
        raise_missing_extra("observability", original)

    assert excinfo.value.__cause__ is original


def test_raise_missing_extra_never_returns():
    sentinel = []

    with pytest.raises(ImportError):
        raise_missing_extra("observability", ImportError("boom"))
        sentinel.append("fell through")  # pragma: no cover

    assert sentinel == []


@pytest.mark.parametrize(
    "extra",
    [
        "observability",
        "observability-tracing-core",
        "observability-flask-tracing",
        "observability-fastapi-tracing",
        "observability-postgres-tracing",
        "observability-langgraph-tracing",
    ],
)
def test_raise_missing_extra_message_names_each_extra(extra):
    with pytest.raises(ImportError) as excinfo:
        raise_missing_extra(extra, ImportError("nope"))

    assert f"pip install 'rococo[{extra}]'" in str(excinfo.value)
