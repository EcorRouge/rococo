"""
``ObservabilityBase`` config validation and setup contract.

The base class does two things in a fixed order: validate that every key in
``REQUIRED_CONFIG_KEYS`` is present *and truthy*, then call ``setup()``. Both
the order and the truthiness rule are load-bearing for every provider, so they
are asserted explicitly here rather than left implicit in the provider tests.
"""

import pytest

from rococo.observability.base import ObservabilityBase


class _Dummy(ObservabilityBase):
    REQUIRED_CONFIG_KEYS = ("A", "B")

    def setup(self):
        self.setup_calls = getattr(self, "setup_calls", 0) + 1


class _NoRequiredKeys(ObservabilityBase):
    REQUIRED_CONFIG_KEYS = ()

    def setup(self):
        self.ran = True


class _NoSetupOverride(ObservabilityBase):
    REQUIRED_CONFIG_KEYS = ()


class _SetupTracker(ObservabilityBase):
    REQUIRED_CONFIG_KEYS = ("A",)
    setup_ran = False

    def setup(self):
        type(self).setup_ran = True


# --------------------------------------------------------------------------
# config storage
# --------------------------------------------------------------------------

def test_init_stores_kwargs_on_config_verbatim():
    instance = _Dummy(A=1, B="two", EXTRA={"nested": True})

    assert instance.config == {"A": 1, "B": "two", "EXTRA": {"nested": True}}


def test_setup_called_exactly_once_when_config_is_complete():
    instance = _Dummy(A=1, B=2)

    assert instance.setup_calls == 1


def test_setup_not_called_when_validation_fails():
    _SetupTracker.setup_ran = False

    with pytest.raises(ValueError):
        _SetupTracker()

    assert _SetupTracker.setup_ran is False, "setup() must run only after validation"


# --------------------------------------------------------------------------
# validation
# --------------------------------------------------------------------------

@pytest.mark.parametrize("missing_key, present", [("A", {"B": 2}), ("B", {"A": 1})])
def test_missing_single_key_raises_value_error_naming_it(missing_key, present):
    with pytest.raises(ValueError) as excinfo:
        _Dummy(**present)

    message = str(excinfo.value)
    assert missing_key in message
    assert "_Dummy" in message
    assert "configuration is incomplete" in message


def test_missing_all_keys_lists_them_comma_separated():
    with pytest.raises(ValueError) as excinfo:
        _Dummy()

    assert "Missing required key(s): A, B." in str(excinfo.value)


@pytest.mark.parametrize("falsy", ["", None, 0, False, [], {}])
def test_falsy_but_present_values_count_as_missing(falsy):
    # `if not self.config.get(key)` — documented deliberately, since a config
    # value of "" or 0 is never meaningful for any of the required keys.
    with pytest.raises(ValueError) as excinfo:
        _Dummy(A=falsy, B=2)

    assert "A" in str(excinfo.value)


def test_empty_required_keys_validates_with_no_config():
    instance = _NoRequiredKeys()

    assert instance.config == {}
    assert instance.ran is True


# --------------------------------------------------------------------------
# setup() contract
# --------------------------------------------------------------------------

def test_subclass_without_setup_override_raises_not_implemented():
    with pytest.raises(NotImplementedError) as excinfo:
        _NoSetupOverride()

    assert "Subclasses must implement" in str(excinfo.value)


def test_base_class_itself_has_no_required_config_keys_attribute():
    # REQUIRED_CONFIG_KEYS is a subclass responsibility; the base class relies
    # on it existing, so instantiating the base directly is an AttributeError.
    assert not hasattr(ObservabilityBase, "REQUIRED_CONFIG_KEYS")

    with pytest.raises(AttributeError):
        ObservabilityBase()
