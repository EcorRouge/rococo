# rococo/observability/__init__.py
from rococo.observability.open_observe import OpenObserve
# from rococo.observability.datadog import Datadog   # future provider, same shape

PROVIDERS = {
    "open_observe": OpenObserve,
}


def get_observability_provider(name):
    try:
        return PROVIDERS[name]
    except KeyError:
        raise ValueError(
            f"Unknown observability provider: {name!r}. Available: {list(PROVIDERS)}"
        )
