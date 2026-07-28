"""
Contract tests for the observability extras declared in ``setup.py``.

The observability module is written to work with only the base ``observability``
extra installed, raising a "pip install 'rococo[<extra>]'" error for anything
heavier. That contract is only as good as the extras themselves, so these tests
pin their names, their contents, how they aggregate, and the two packaging
subtleties that would otherwise regress silently: the prerelease-inclusive lower
bounds on ``opentelemetry-instrumentation-*`` and the langgraph python marker.
"""

import runpy
from unittest.mock import MagicMock, patch

import pytest
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

EXPECTED_VERSION = "1.3.4"

OBSERVABILITY_EXTRAS = (
    "observability",
    "observability-tracing-core",
    "observability-flask-tracing",
    "observability-fastapi-tracing",
    "observability-postgres-tracing",
    "observability-langgraph-tracing",
    "observability-tracing",
)

_CACHED_KWARGS = None


def load_setup_kwargs(repo_root, monkeypatch):
    """Runs setup.py with setuptools.setup mocked out and returns its kwargs."""
    global _CACHED_KWARGS
    if _CACHED_KWARGS is None:
        # setup.py does open('README.md').read(), so it must run from the repo root.
        monkeypatch.chdir(repo_root)
        fake_setup = MagicMock()
        with patch("setuptools.setup", fake_setup):
            runpy.run_path(str(repo_root / "setup.py"), run_name="__main__")
        assert fake_setup.call_count == 1
        _CACHED_KWARGS = dict(fake_setup.call_args.kwargs)
    return _CACHED_KWARGS


@pytest.fixture
def setup_kwargs(repo_root, monkeypatch):
    return load_setup_kwargs(repo_root, monkeypatch)


@pytest.fixture
def extras(setup_kwargs):
    return setup_kwargs["extras_require"]


def names_of(requirements):
    return {canonicalize_name(Requirement(r).name) for r in requirements}


# --------------------------------------------------------------------------
# version
# --------------------------------------------------------------------------

def test_version_is_three_part_dotted(setup_kwargs):
    version = setup_kwargs["version"]
    assert version
    parts = version.split(".")
    assert len(parts) == 3, version
    assert all(part.isdigit() for part in parts), version


def test_version_matches_expected_bump(setup_kwargs):
    # Deliberately exact: a future bump should fail here and be updated knowingly.
    assert setup_kwargs["version"] == EXPECTED_VERSION


# --------------------------------------------------------------------------
# extras exist / contents
# --------------------------------------------------------------------------

@pytest.mark.parametrize("extra", OBSERVABILITY_EXTRAS)
def test_observability_extra_declared(extras, extra):
    assert extra in extras
    assert extras[extra], f"{extra} must not be empty"


def test_base_observability_extra_contents(extras):
    assert names_of(extras["observability"]) == {"requests", "opentelemetry-api"}


def test_tracing_core_extra_contents(extras):
    assert names_of(extras["observability-tracing-core"]) == {
        "opentelemetry-sdk",
        "opentelemetry-exporter-otlp-proto-http",
    }


def test_flask_tracing_extra_contents(extras):
    assert names_of(extras["observability-flask-tracing"]) == {
        "opentelemetry-instrumentation-flask",
        "opentelemetry-instrumentation-requests",
        "werkzeug",  # declared as 'Werkzeug'; canonicalized for comparison
    }


def test_fastapi_tracing_extra_contents(extras):
    assert names_of(extras["observability-fastapi-tracing"]) == {
        "opentelemetry-instrumentation-fastapi",
        "opentelemetry-instrumentation-httpx",
        "opentelemetry-instrumentation-requests",
    }


def test_postgres_tracing_extra_contents(extras):
    assert names_of(extras["observability-postgres-tracing"]) == {
        "opentelemetry-instrumentation-psycopg2",
    }


def test_langgraph_tracing_extra_contents(extras):
    assert names_of(extras["observability-langgraph-tracing"]) == {
        "openinference-instrumentation-langchain",
    }


def test_langgraph_requirement_keeps_python_version_marker(extras):
    requirement = Requirement(extras["observability-langgraph-tracing"][0])
    assert requirement.marker is not None
    marker = str(requirement.marker)
    assert "python_version" in marker
    assert "3.15" in marker


# --------------------------------------------------------------------------
# aggregation
# --------------------------------------------------------------------------

def test_tracing_aggregate_is_union_of_parts(extras):
    # Sets, not lists: opentelemetry-instrumentation-requests legitimately
    # appears in both the flask and fastapi extras.
    assert set(extras["observability-tracing"]) == (
        set(extras["observability-tracing-core"])
        | set(extras["observability-flask-tracing"])
        | set(extras["observability-fastapi-tracing"])
        | set(extras["observability-postgres-tracing"])
        | set(extras["observability-langgraph-tracing"])
    )


def test_all_extra_includes_observability(extras):
    assert set(extras["all"]) >= set(extras["observability"]) | set(extras["observability-tracing"])


@pytest.mark.parametrize("extra", ["data", "emailing", "messaging", "faxing", "sms"])
def test_all_extra_still_includes_preexisting_extras(extras, extra):
    assert set(extras["all"]) >= set(extras[extra])


def test_data_extra_includes_dynamodb(extras):
    assert set(extras["data"]) >= set(extras["data-dynamodb"])


# --------------------------------------------------------------------------
# requirement hygiene
# --------------------------------------------------------------------------

@pytest.mark.parametrize("extra", OBSERVABILITY_EXTRAS)
def test_observability_requirements_are_valid_pep508(extras, extra):
    for raw in extras[extra]:
        Requirement(raw)  # raises InvalidRequirement on a typo


@pytest.mark.parametrize("extra", OBSERVABILITY_EXTRAS)
def test_instrumentation_lower_bounds_are_prerelease_inclusive(extras, extra):
    """
    opentelemetry-instrumentation-* only ever publishes prereleases (0.45b0 …),
    so a plain '>=0.45' matches nothing and installers resolve no candidate.
    """
    from packaging.version import Version

    checked = 0
    for raw in extras[extra]:
        requirement = Requirement(raw)
        if not canonicalize_name(requirement.name).startswith("opentelemetry-instrumentation-"):
            continue
        lower_bounds = [s for s in requirement.specifier if s.operator == ">="]
        assert lower_bounds, f"{raw} has no >= lower bound"
        for spec in lower_bounds:
            assert Version(spec.version).is_prerelease, (
                f"{raw}: lower bound {spec.version!r} is not prerelease-inclusive"
            )
        checked += 1

    if extra in ("observability", "observability-tracing-core", "observability-langgraph-tracing"):
        assert checked == 0
    else:
        assert checked > 0


@pytest.mark.parametrize("extra", OBSERVABILITY_EXTRAS)
def test_observability_requirements_have_upper_bounds(extras, extra):
    for raw in extras[extra]:
        requirement = Requirement(raw)
        operators = {s.operator for s in requirement.specifier}
        assert "<" in operators, f"{raw} has no upper bound"
