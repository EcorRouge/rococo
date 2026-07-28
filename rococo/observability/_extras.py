"""
Helpers for turning missing optional dependencies into clear, actionable errors.

The observability module's third-party dependencies are all optional extras (see
``extras_require`` in ``setup.py``). Whenever one of those imports fails, we
re-raise as an ``ImportError`` that names the exact extra to install, rather than
leaking a bare ``No module named ...`` that gives the caller no idea which
``pip install 'rococo[...]'`` would fix it.
"""


def raise_missing_extra(extra, error):
    """
    Re-raise a failed optional import as a descriptive ``ImportError``.

    Args:
        extra: The name of the extra in ``setup.py`` that provides the missing
            dependency (e.g. ``"observability-flask-tracing"``).
        error: The original ``ImportError`` that was caught.
    """
    raise ImportError(
        f"This feature of rococo.observability requires the optional "
        f"'{extra}' dependencies, which are not installed. Install them with:\n\n"
        f"    pip install 'rococo[{extra}]'\n\n"
        f"(underlying import error: {error})"
    ) from error
