import pytest

from argus_nifi_config.catalog import BY_FILENAME, nifi_properties
from argus_nifi_config.model import Setting, ValueType


def test_int_validation():
    s = Setting("k", "K", "g", type=ValueType.INT)
    assert s.validate("123") is None
    assert s.validate("-5") is None
    assert s.validate("abc") is not None
    assert s.validate("") is None  # 빈 값 허용


def test_bool_validation():
    s = Setting("k", "K", "g", type=ValueType.BOOL)
    assert s.validate("true") is None
    assert s.validate("FALSE") is None
    assert s.validate("yes") is not None


def test_enum_validation():
    s = Setting("k", "K", "g", type=ValueType.ENUM, choices=("A", "B"))
    assert s.validate("A") is None
    assert s.validate("C") is not None


def test_duration_and_datasize():
    d = Setting("k", "K", "g", type=ValueType.DURATION)
    assert d.validate("30 sec") is None
    assert d.validate("1 min") is None
    assert d.validate("5 bananas") is not None
    z = Setting("k", "K", "g", type=ValueType.DATASIZE)
    assert z.validate("1 GB") is None
    assert z.validate("500 MB") is None
    assert z.validate("big") is not None


def test_sensitive_masking():
    s = Setting("k", "K", "g", sensitive=True)
    assert s.display_value("secret") == "•" * 8
    assert s.display_value("") == ""


def test_custom_validator_sensitive_props_key():
    s = nifi_properties.BY_KEY["nifi.sensitive.props.key"]
    assert s.validate("short") is not None
    assert s.validate("a-long-enough-key") is None


def test_catalog_no_duplicate_keys():
    keys = [s.key for s in nifi_properties.SETTINGS]
    assert len(keys) == len(set(keys))


def test_registry_has_expected_files():
    for fn in ("nifi.properties", "bootstrap.conf", "authorizers.xml",
               "login-identity-providers.xml", "state-management.xml"):
        assert fn in BY_FILENAME
