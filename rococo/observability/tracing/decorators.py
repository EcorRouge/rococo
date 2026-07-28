from functools import wraps

from .._extras import raise_missing_extra

try:
    from opentelemetry import trace
except ImportError as e:
    # opentelemetry-api ships with the base 'observability' extra.
    raise_missing_extra("observability", e)


def traced(name=None):
    """
    Wraps a function in its own span. Use on the direct calls made by a
    view function/worker entrypoint — not recursively on everything they
    call internally, to keep traces readable and avoid over-instrumenting.

        @traced()
        def fetch_current_and_prior_flag_metrics(dates):
            ...

    Pass `name` to override the span name (defaults to the function's
    module-qualified name).
    """
    def decorator(func):
        span_name = name or f"{func.__module__}.{func.__qualname__}"

        @wraps(func)
        def wrapper(*args, **kwargs):
            tracer = trace.get_tracer(func.__module__)
            with tracer.start_as_current_span(span_name):
                return func(*args, **kwargs)
        return wrapper
    return decorator

def traced_step(name):
    """
    Context manager for tracing a specific *portion* of a function's body,
    as a sibling span rather than wrapping the whole function. Use inside
    decorators/middleware where you want to isolate your own overhead
    (e.g. an auth check) from the wrapped function's own execution time.

        def login_required(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                with traced_step("login_required"):
                    if not is_authenticated():
                        abort(401)
                return func(*args, **kwargs)
            return wrapper
    """
    tracer = trace.get_tracer(__name__)
    return tracer.start_as_current_span(name)