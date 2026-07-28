"""
``OpenObserveHandler``.

Every handler is built through the ``handler`` factory fixture, which closes it
in teardown: the constructor starts a daemon thread and registers an ``atexit``
hook, so an unclosed handler leaks a thread and an interpreter-shutdown callback
that would try to reach the network. ``requests.post`` stays mocked throughout
by the autouse ``no_network`` fixture.
"""

import base64
import logging
import queue
import time
from unittest.mock import MagicMock

import pytest
import requests

from rococo.observability.logging import open_observe_handler as handler_module
from rococo.observability.logging.open_observe_handler import OpenObserveHandler
from rococo.observability.open_observe import OpenObserve

BASE_URL = "http://h:5080"
ORG = "myorg"
TOKEN = "mytoken"


@pytest.fixture
def make_handler():
    """Factory that guarantees close() for every handler it builds."""
    created = []

    def make(**overrides):
        kwargs = {
            "base_url": BASE_URL,
            "org_id": ORG,
            "ingestion_token": TOKEN,
            "env": "test",
        }
        kwargs.update(overrides)
        handler = OpenObserveHandler(**kwargs)
        created.append(handler)
        return handler

    yield make

    for handler in created:
        handler.close()


@pytest.fixture
def handler(make_handler):
    return make_handler()


def make_record(msg="hello", args=None, level=logging.INFO, exc_info=None):
    return logging.LogRecord(
        name="test.logger",
        level=level,
        pathname=__file__,
        lineno=10,
        msg=msg,
        args=args,
        exc_info=exc_info,
    )


@pytest.fixture
def valid_span_stub(monkeypatch):
    """
    Replaces the handler module's `trace` with a fake exposing a span whose
    context carries known ids. The handler binds `trace` at import time, so the
    module attribute is the seam — not sys.modules.
    """

    def install(is_valid=True, trace_id=0x1234, span_id=0xABCD):
        span_context = MagicMock()
        span_context.is_valid = is_valid
        span_context.trace_id = trace_id
        span_context.span_id = span_id
        span = MagicMock()
        span.get_span_context.return_value = span_context
        fake_trace = MagicMock()
        fake_trace.get_current_span.return_value = span
        monkeypatch.setattr(handler_module, "trace", fake_trace)
        return span

    return install


# --------------------------------------------------------------------------
# URL / auth / defaults
# --------------------------------------------------------------------------

def test_url_construction(handler):
    assert handler.url == "http://h:5080/api/myorg/app-logs/_json"


def test_url_construction_with_trailing_slash(make_handler):
    assert make_handler(base_url="http://h:5080/").url == "http://h:5080/api/myorg/app-logs/_json"


def test_url_construction_with_custom_stream(make_handler):
    assert make_handler(stream="mystream").url == "http://h:5080/api/myorg/mystream/_json"


def test_url_construction_with_path_prefix(make_handler):
    # urljoin() resolves the relative path against the base *directory*, so a
    # path prefix is preserved only because of the enforced trailing slash.
    assert make_handler(base_url="http://h:5080/oo").url == "http://h:5080/oo/api/myorg/app-logs/_json"


def test_auth_header_is_basic_base64(handler):
    expected = "Basic " + base64.b64encode(f"{ORG}:{TOKEN}".encode()).decode()

    assert handler.auth_header == expected


def test_defaults(handler):
    assert handler.url.endswith("/app-logs/_json")
    assert handler.timeout == 2
    assert handler.batch_size == 50
    assert handler._queue.maxsize == 10_000
    assert handler.env == "test"
    assert handler.service_name is None


def test_overrides(make_handler):
    handler = make_handler(
        stream="s", service_name="svc", timeout=9, queue_maxsize=7, batch_size=3
    )

    assert handler.timeout == 9
    assert handler.batch_size == 3
    assert handler._queue.maxsize == 7
    assert handler.service_name == "svc"


def test_handler_is_a_logging_handler(handler):
    assert isinstance(handler, logging.Handler)


# --------------------------------------------------------------------------
# worker thread
# --------------------------------------------------------------------------

def test_worker_thread_started_as_named_daemon(handler):
    assert handler._worker.is_alive()
    assert handler._worker.daemon is True
    assert handler._worker.name == "OpenObserveHandler-sender"


# --------------------------------------------------------------------------
# emit() entry shape
# --------------------------------------------------------------------------

def test_emit_entry_shape(handler, valid_span_stub):
    valid_span_stub(is_valid=False)
    before = int(time.time() * 1_000_000)

    handler.emit(make_record(msg="%s-%s", args=("a", "b"), level=logging.WARNING))

    entry = handler._queue.get_nowait()
    after = int(time.time() * 1_000_000)
    assert before <= entry["_timestamp"] <= after
    assert isinstance(entry["_timestamp"], int)
    assert entry["level"] == "WARNING"
    assert entry["message"] == "a-b", "message must be the formatted record"
    assert entry["env"] == "test"


def test_emit_omits_service_when_unset(handler, valid_span_stub):
    valid_span_stub(is_valid=False)

    handler.emit(make_record())

    assert "service" not in handler._queue.get_nowait()


def test_emit_includes_service_when_set(make_handler, valid_span_stub):
    valid_span_stub(is_valid=False)
    handler = make_handler(service_name="svc")

    handler.emit(make_record())

    assert handler._queue.get_nowait()["service"] == "svc"


def test_emit_omits_stack_trace_without_exc_info(handler, valid_span_stub):
    valid_span_stub(is_valid=False)

    handler.emit(make_record())

    assert "stack_trace" not in handler._queue.get_nowait()


def test_emit_includes_stack_trace_with_exc_info(handler, valid_span_stub):
    valid_span_stub(is_valid=False)
    try:
        raise ValueError("kaboom")
    except ValueError:
        import sys

        record = make_record(msg="failed", level=logging.ERROR, exc_info=sys.exc_info())

    handler.emit(record)

    entry = handler._queue.get_nowait()
    assert entry["stack_trace"] == handler.format(record)
    assert "ValueError: kaboom" in entry["stack_trace"]


# --------------------------------------------------------------------------
# trace correlation
# --------------------------------------------------------------------------

def test_emit_adds_trace_correlation_for_valid_span(handler, valid_span_stub):
    valid_span_stub(is_valid=True, trace_id=0x1234, span_id=0xABCD)

    handler.emit(make_record())

    entry = handler._queue.get_nowait()
    assert entry["trace_id"] == "0" * 28 + "1234"
    assert len(entry["trace_id"]) == 32
    assert entry["span_id"] == "0" * 12 + "abcd"
    assert len(entry["span_id"]) == 16


def test_emit_omits_trace_correlation_for_invalid_span(handler, valid_span_stub):
    valid_span_stub(is_valid=False)

    handler.emit(make_record())

    entry = handler._queue.get_nowait()
    assert "trace_id" not in entry
    assert "span_id" not in entry


def test_emit_never_raises_over_a_missing_extra(handler, valid_span_stub, block_modules):
    """
    The opentelemetry check happens at import time, so blocking the package
    afterwards must not turn a log call into an exception: errors raised from
    inside emit() can be swallowed by logging.Handler.handleError.
    """
    valid_span_stub(is_valid=True)
    block_modules("opentelemetry", reset=())

    handler.emit(make_record(msg="still-logged"))  # must not raise

    assert handler._queue.get_nowait()["message"] == "still-logged"


def test_emit_does_not_import_anything_per_record(handler, valid_span_stub, monkeypatch):
    # `trace` is bound once at import time rather than re-imported per record —
    # emit() runs on the caller's thread, so it stays allocation-cheap.
    valid_span_stub(is_valid=False)
    import builtins

    original_import = builtins.__import__

    def fail_on_opentelemetry(name, *args, **kwargs):
        assert not name.startswith("opentelemetry"), "emit() must not import per record"
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fail_on_opentelemetry)

    handler.emit(make_record())

    assert handler._queue.qsize() == 1


# --------------------------------------------------------------------------
# queue behavior
# --------------------------------------------------------------------------

def stop_worker(handler):
    """Stops the drain thread so queue contents can be inspected reliably."""
    handler._stop_event.set()
    handler._worker.join(timeout=5)
    assert not handler._worker.is_alive()


def test_emit_drops_record_when_queue_is_full(make_handler, valid_span_stub):
    valid_span_stub(is_valid=False)
    handler = make_handler(queue_maxsize=1)
    stop_worker(handler)

    handler.emit(make_record(msg="first"))
    assert handler._queue.qsize() == 1

    handler.emit(make_record(msg="dropped"))  # must not raise

    assert handler._queue.qsize() == 1
    assert handler._queue.get_nowait()["message"] == "first"


def test_drain_batch_returns_empty_list_on_empty_queue(handler):
    stop_worker(handler)

    assert handler._drain_batch() == []


def test_drain_batch_caps_at_batch_size_and_leaves_remainder(make_handler):
    handler = make_handler(batch_size=2)
    stop_worker(handler)
    for i in range(5):
        handler._queue.put_nowait({"message": i})

    batch = handler._drain_batch()

    assert len(batch) == 2
    assert [entry["message"] for entry in batch] == [0, 1]
    assert handler._queue.qsize() == 3


def test_queue_is_a_bounded_queue(handler):
    assert isinstance(handler._queue, queue.Queue)


# --------------------------------------------------------------------------
# _send
# --------------------------------------------------------------------------

def test_send_posts_batch(handler, no_network):
    stop_worker(handler)
    batch = [{"message": "one"}]

    handler._send(batch)

    no_network.assert_called_once_with(
        handler.url,
        json=batch,
        headers={"Authorization": handler.auth_header, "Content-Type": "application/json"},
        timeout=handler.timeout,
    )


def test_send_swallows_request_exception(handler, no_network):
    stop_worker(handler)
    no_network.side_effect = requests.RequestException("boom")

    handler._send([{"message": "one"}])  # must not raise


def test_send_does_not_swallow_other_exceptions(handler, no_network):
    stop_worker(handler)
    no_network.side_effect = ValueError("not a request error")

    # Documents the current narrow `except requests.RequestException`.
    with pytest.raises(ValueError):
        handler._send([{"message": "one"}])


# --------------------------------------------------------------------------
# close() / draining
# --------------------------------------------------------------------------

def test_close_drains_queued_records(make_handler, valid_span_stub, no_network):
    valid_span_stub(is_valid=False)
    handler = make_handler()

    handler.emit(make_record(msg="drain-me"))
    handler.close()

    posted_messages = [
        entry["message"]
        for call in no_network.call_args_list
        for entry in call.kwargs["json"]
    ]
    assert "drain-me" in posted_messages


def test_close_stops_the_worker(handler):
    handler.close()

    assert handler._stop_event.is_set()
    assert not handler._worker.is_alive()


def test_close_is_idempotent(handler):
    handler.close()
    handler.close()  # also invoked via atexit — must not raise


# --------------------------------------------------------------------------
# integration with the provider
# --------------------------------------------------------------------------

def test_provider_returns_a_real_configured_handler(valid_config):
    provider = OpenObserve(**valid_config)

    handler = provider.get_logging_handler()
    try:
        assert isinstance(handler, OpenObserveHandler)
        assert handler.url == "http://openobserve.test:5080/api/testorg/logs/_json"
        assert handler.env == valid_config["APP_ENV"]
        assert handler.service_name == valid_config["SERVICE_NAME"]
    finally:
        handler.close()
