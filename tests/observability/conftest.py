"""
Shared fixtures for the rococo.observability test package.

The module under test is deliberately layered so that importing
``rococo.observability`` needs only the base ``observability`` extra and every
heavier dependency is imported lazily (or guarded by ``try/except ImportError``
that re-raises through ``_extras.raise_missing_extra``). These fixtures make
that contract testable regardless of what happens to be installed in the dev
venv: ``block_modules`` hides real packages, ``stub_module`` fakes absent ones.

Every fixture restores ``sys.modules`` / ``sys.meta_path`` on teardown — a
leaked import blocker would break unrelated tests in the same session.
"""

import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

import pytest


@pytest.fixture(scope="session")
def repo_root():
    return Path(__file__).resolve().parents[2]


@pytest.fixture
def valid_config():
    """All five of OpenObserve.REQUIRED_CONFIG_KEYS, with harmless test values."""
    return {
        "OO_BASE_URL": "http://openobserve.test:5080",
        "OO_ORG_ID": "testorg",
        "OO_INGESTION_TOKEN": "testtoken",
        "SERVICE_NAME": "test-service",
        "APP_ENV": "test",
    }


class _ImportBlocker:
    """A sys.meta_path finder that refuses to find the given module prefixes."""

    def __init__(self, prefixes):
        self.prefixes = tuple(prefixes)

    def _blocked(self, name):
        return any(name == p or name.startswith(p + ".") for p in self.prefixes)

    def find_spec(self, fullname, path=None, target=None):
        if self._blocked(fullname):
            raise ImportError(f"No module named {fullname!r}")
        return None


def _matching_modules(prefixes):
    return [
        name for name in list(sys.modules)
        if any(name == p or name.startswith(p + ".") for p in prefixes)
    ]


@pytest.fixture
def block_modules():
    """
    Factory fixture: ``block("opentelemetry", "werkzeug")`` makes those imports
    fail with ImportError for the duration of the test.

    Already-imported modules matching the prefixes are evicted from
    ``sys.modules``, as is everything matching ``reset`` (by default the whole
    ``rococo.observability`` package), so the guarded ``try/except ImportError``
    blocks re-execute on the next import.

    Narrow ``reset`` when the code under test is *already* imported and only a
    lazy import should fail: evicting ``rococo.observability`` would otherwise
    make the handler's own ``from ._extras import ...`` re-run the package
    ``__init__``, and the resulting error would name the wrong extra.
    """
    saved_modules = dict(sys.modules)
    saved_meta_path = list(sys.meta_path)
    installed = []

    def block(*prefixes, reset=("rococo.observability",)):
        blocker = _ImportBlocker(prefixes)
        sys.meta_path.insert(0, blocker)
        installed.append(blocker)
        for name in _matching_modules(prefixes) + _matching_modules(reset):
            sys.modules.pop(name, None)
        return blocker

    yield block

    for blocker in installed:
        if blocker in sys.meta_path:
            sys.meta_path.remove(blocker)
    sys.meta_path[:] = saved_meta_path
    sys.modules.clear()
    sys.modules.update(saved_modules)


@pytest.fixture
def stub_module():
    """
    Factory fixture: ``stub("opentelemetry.instrumentation.flask", FlaskInstrumentor=...)``
    registers a fake module (and any missing parent packages) in ``sys.modules``.

    Used to fake instrumentor packages, ``werkzeug.wrappers`` and the
    SDK-dependent tracer module without installing anything.
    """
    saved_modules = {}
    saved_attrs = {}
    _MISSING = object()

    def _remember_module(name):
        if name not in saved_modules:
            saved_modules[name] = sys.modules.get(name, _MISSING)

    def _remember_attr(parent, attr):
        if (parent.__name__, attr) not in saved_attrs:
            saved_attrs[(parent.__name__, attr)] = (parent, getattr(parent, attr, _MISSING))

    def stub(name, **attrs):
        parts = name.split(".")
        # Ensure every parent package exists, creating placeholders as needed.
        for i in range(1, len(parts)):
            parent_name = ".".join(parts[:i])
            if parent_name not in sys.modules:
                _remember_module(parent_name)
                parent = types.ModuleType(parent_name)
                parent.__path__ = []  # mark as a package so submodule imports work
                sys.modules[parent_name] = parent

        _remember_module(name)
        module = types.ModuleType(name)
        module.__path__ = []
        for key, value in attrs.items():
            setattr(module, key, value)
        sys.modules[name] = module

        if len(parts) > 1:
            parent = sys.modules[".".join(parts[:-1])]
            _remember_attr(parent, parts[-1])
            setattr(parent, parts[-1], module)
        return module

    yield stub

    for (_parent_name, attr), (parent, previous) in saved_attrs.items():
        if previous is _MISSING:
            try:
                delattr(parent, attr)
            except AttributeError:  # pragma: no cover - already gone
                pass
        else:
            setattr(parent, attr, previous)
    for name, previous in saved_modules.items():
        if previous is _MISSING:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    """
    Autouse safety net: the logging handler starts a background thread that
    POSTs on its own, so ``requests.post`` is mocked for every test in this
    package even if the test itself forgets to patch it.
    """
    post = MagicMock()
    monkeypatch.setattr(
        "rococo.observability.logging.open_observe_handler.requests.post",
        post,
        raising=True,
    )
    return post


@pytest.fixture
def reset_observability_modules():
    """
    Evicts every ``rococo.observability*`` module so the next import runs the
    module body again (needed when a test observes import-time behavior).
    """
    saved = dict(sys.modules)
    yield
    sys.modules.clear()
    sys.modules.update(saved)
