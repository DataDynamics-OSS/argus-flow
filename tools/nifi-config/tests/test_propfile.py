from pathlib import Path

from argus_nifi_config.propfile import PropertiesFile

SAMPLE = """\
# NiFi properties
nifi.web.https.port=8443
# comment
nifi.cluster.is.node=false

nifi.sensitive.props.key=
"""


def _write(tmp_path: Path, text: str) -> Path:
    p = tmp_path / "nifi.properties"
    p.write_text(text, encoding="utf-8")
    return p


def test_roundtrip_preserves_unchanged(tmp_path):
    p = _write(tmp_path, SAMPLE)
    pf = PropertiesFile.load(p)
    assert pf.dumps() == SAMPLE


def test_set_existing_key_in_place(tmp_path):
    pf = PropertiesFile.load(_write(tmp_path, SAMPLE))
    pf.set("nifi.web.https.port", "9443")
    out = pf.dumps()
    assert "nifi.web.https.port=9443" in out
    # 주석과 순서 보존
    assert out.index("# comment") < out.index("nifi.cluster.is.node")
    assert pf.changes() == [("nifi.web.https.port", "8443", "9443")]


def test_set_new_key_appends_managed_section(tmp_path):
    pf = PropertiesFile.load(_write(tmp_path, SAMPLE))
    pf.set("nifi.web.http.port", "8080")
    out = pf.dumps()
    assert "argus-nifi-config 관리 섹션" in out
    assert out.rstrip().endswith("nifi.web.http.port=8080")
    assert ("nifi.web.http.port", None, "8080") in pf.changes()


def test_get_and_unset(tmp_path):
    pf = PropertiesFile.load(_write(tmp_path, SAMPLE))
    assert pf.get("nifi.cluster.is.node") == "false"
    assert pf.unset("nifi.cluster.is.node") is True
    assert pf.get("nifi.cluster.is.node") is None
    assert pf.unset("does.not.exist") is False


def test_empty_value_key_parsed(tmp_path):
    pf = PropertiesFile.load(_write(tmp_path, SAMPLE))
    assert pf.get("nifi.sensitive.props.key") == ""
