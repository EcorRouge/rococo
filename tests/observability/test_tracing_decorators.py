"""
``rococo.observability.tracing.decorators`` — ``traced`` and ``traced_step``.

Only the opentelemetry *API* is needed here (no SDK), so the tracer is replaced
with a MagicMock and the assertions are about span names and transparency.
"""

from unittest.mock import MagicMock

import pytest

pytest.importorskip("opentelemetry", reason="requires the 'observability' extra")

from rococo.observability.tracing import decorators  # noqa: E402
from rococo.observability.tracing.decorators import traced, traced_step  # noqa: E402


@pytest.fixture
def fake_trace(monkeypatch):
    """Replaces the module-level `trace` in decorators.py with a mock."""
    tracer = MagicMock(name="tracer")
    fake = MagicMock(name="trace")
    fake.get_tracer.return_value = tracer
    monkeypatch.setattr(decorators, "trace", fake)
    fake.tracer = tracer
    return fake


def span_name_of(fake_trace):
    return fake_trace.tracer.start_as_current_span.call_args.args[0]


# --------------------------------------------------------------------------
# traced() span naming
# --------------------------------------------------------------------------

def test_traced_default_span_name_is_module_qualified(fake_trace):
    @traced()
    def sample():
        return "value"

    assert sample() == "value"
    assert span_name_of(fake_trace) == f"{sample.__module__}.{sample.__qualname__}"


def test_traced_explicit_name_overrides(fake_trace):
    @traced(name="custom")
    def sample():
        return None

    sample()

    assert span_name_of(fake_trace) == "custom"


def test_traced_gets_tracer_for_the_decorated_functions_module(fake_trace):
    @traced()
    def sample():
        return None

    sample()

    fake_trace.get_tracer.assert_called_with(sample.__wrapped__.__module__)


def test_traced_uses_qualname_for_methods(fake_trace):
    class Thing:
        @traced()
        def method(self):
            return "m"

    Thing().method()

    name = span_name_of(fake_trace)
    assert name.endswith("Thing.method")
    assert "Thing" in name, "the decorator uses __qualname__ so the class is visible"


def test_traced_uses_qualname_for_nested_functions(fake_trace):
    def outer():
        @traced()
        def inner():
            return "i"

        return inner

    outer()()

    name = span_name_of(fake_trace)
    assert "outer.<locals>.inner" in name


# --------------------------------------------------------------------------
# traced() transparency
# --------------------------------------------------------------------------

def test_traced_passes_through_return_value_and_arguments(fake_trace):
    @traced()
    def add(a, b, c=0):
        return a + b + c

    assert add(1, 2, c=3) == 6


def test_traced_preserves_metadata(fake_trace):
    def original():
        """Docstring."""

    decorated = traced()(original)

    assert decorated.__name__ == "original"
    assert decorated.__doc__ == "Docstring."
    assert decorated.__wrapped__ is original


def test_traced_propagates_exceptions_and_closes_the_span(fake_trace):
    boom = RuntimeError("boom")

    @traced()
    def failing():
        raise boom

    with pytest.raises(RuntimeError) as excinfo:
        failing()

    assert excinfo.value is boom
    context_manager = fake_trace.tracer.start_as_current_span.return_value
    assert context_manager.__enter__.called
    assert context_manager.__exit__.called


def test_traced_span_name_computed_once_at_decoration_time(fake_trace):
    @traced()
    def sample():
        return None

    sample()
    first = span_name_of(fake_trace)
    sample()

    assert span_name_of(fake_trace) == first


# --------------------------------------------------------------------------
# traced_step()
# --------------------------------------------------------------------------

def test_traced_step_returns_the_tracers_context_manager(fake_trace):
    result = traced_step("step-name")

    fake_trace.tracer.start_as_current_span.assert_called_once_with("step-name")
    assert result is fake_trace.tracer.start_as_current_span.return_value


def test_traced_step_is_usable_as_a_context_manager(fake_trace):
    with traced_step("step-name"):
        pass

    context_manager = fake_trace.tracer.start_as_current_span.return_value
    assert context_manager.__enter__.called
    assert context_manager.__exit__.called


def test_traced_step_tracer_is_named_after_the_decorators_module(fake_trace):
    # Current behavior: the tracer is obtained for decorators.__name__, not the
    # caller's module, so every traced_step span shares one instrumentation name.
    traced_step("step-name")

    fake_trace.get_tracer.assert_called_once_with(decorators.__name__)


# --------------------------------------------------------------------------
# open_observe_tracer re-exports the same traced()
# --------------------------------------------------------------------------

def test_open_observe_tracer_reexports_the_one_traced():
    """
    open_observe_tracer used to define a second, subtly different `traced`
    (span names from __name__ rather than __qualname__, so two same-named
    methods on different classes collided). It must stay a re-export of the
    single implementation so the import path can't change span naming.
    """
    pytest.importorskip(
        "opentelemetry.sdk", reason="requires the 'observability-tracing-core' extra"
    )
    from rococo.observability.tracing import open_observe_tracer

    assert open_observe_tracer.traced is traced


def test_reexported_traced_uses_qualname(monkeypatch, fake_trace):
    pytest.importorskip(
        "opentelemetry.sdk", reason="requires the 'observability-tracing-core' extra"
    )
    from rococo.observability.tracing import open_observe_tracer

    class Thing:
        @open_observe_tracer.traced()
        def method(self):
            return "m"

    Thing().method()

    assert span_name_of(fake_trace).endswith("Thing.method")
