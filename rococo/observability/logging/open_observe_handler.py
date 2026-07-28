import atexit
import base64
import logging
import queue
import threading
import time
from urllib.parse import urljoin

from .._extras import raise_missing_extra

try:
    import requests
except ImportError as e:
    # 'requests' is the hard dependency of the base logging handler's transport.
    raise_missing_extra("observability", e)

try:
    from opentelemetry import trace
except ImportError as e:
    # Every emitted record is correlated with the active trace, so the
    # opentelemetry API is a hard requirement too (it ships in the same base
    # 'observability' extra). Fail here, at import time, rather than from
    # inside emit() — handler errors raised during logging are swallowed by
    # logging.Handler.handleError in some configurations.
    raise_missing_extra("observability", e)


class OpenObserveHandler(logging.Handler):
    """
    Ships log records to an OpenObserve logs stream via HTTP ingestion.

    Non-blocking by design: emit() only builds the log entry and pushes it
    onto an in-memory queue — a single background thread drains the queue
    and does the actual HTTP POST (batched), so callers never block on
    network I/O. Mirrors pyrollbar's default 'thread' handler behavior.
    """

    def __init__(self, base_url, org_id, ingestion_token, env, stream="app-logs",
                 service_name=None, timeout=2, queue_maxsize=10_000, batch_size=50):
        super().__init__()
        self.url = urljoin(
            base_url.rstrip("/") + "/",   # ensure exactly one trailing slash
            f"api/{org_id}/{stream}/_json",  # no leading slash — relative to base
        )
        self.auth_header = "Basic " + base64.b64encode((org_id + ':' + ingestion_token).encode()).decode()
        self.env = env
        self.service_name = service_name
        self.timeout = timeout
        self.batch_size = batch_size

        self._queue = queue.Queue(maxsize=queue_maxsize)
        self._stop_event = threading.Event()
        self._worker = threading.Thread(
            target=self._process_queue,
            name="OpenObserveHandler-sender",
            daemon=True,
        )
        self._worker.start()

        # Ensure a normal process exit gives the queue a chance to drain
        atexit.register(self.close)

    def emit(self, record):
        log_entry = {
            "_timestamp": int(time.time() * 1_000_000),
            "level": record.levelname,
            "message": record.getMessage(),
            "env": self.env,
        }
        if self.service_name:
            log_entry["service"] = self.service_name
        if record.exc_info:
            log_entry["stack_trace"] = self.format(record)

        # Correlate with the active trace, if one exists.
        span = trace.get_current_span()
        span_context = span.get_span_context()
        if span_context.is_valid:
            log_entry["trace_id"] = format(span_context.trace_id, "032x")
            log_entry["span_id"] = format(span_context.span_id, "016x")

        try:
            self._queue.put_nowait(log_entry)
        except queue.Full:
            # Queue backed up (OpenObserve unreachable/slow for a while) —
            # drop rather than block the caller.
            pass

    def _process_queue(self):
        """Runs on the background thread: drains the queue, sends batches."""
        while not self._stop_event.is_set() or not self._queue.empty():
            batch = self._drain_batch()
            if batch:
                self._send(batch)
            else:
                time.sleep(0.1)   # nothing to do — avoid a busy loop

    def _drain_batch(self):
        """Pulls up to self.batch_size entries currently on the queue, non-blocking."""
        batch = []
        try:
            while len(batch) < self.batch_size:
                batch.append(self._queue.get_nowait())
        except queue.Empty:
            pass
        return batch

    def _send(self, batch):
        try:
            requests.post(
                self.url,
                json=batch,
                headers={"Authorization": self.auth_header, "Content-Type": "application/json"},
                timeout=self.timeout,
            )
        except requests.RequestException:
            pass

    def close(self):
        """
        Signals the background thread to finish draining and stop, then
        waits briefly for it. Registered via atexit so a normal process
        exit flushes pending logs; a hard kill (SIGKILL) still loses
        whatever's in the queue at that instant, same as any in-memory queue.
        """
        self._stop_event.set()
        self._worker.join(timeout=5)
        super().close()
