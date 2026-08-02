from pathlib import Path

import pytest

from argus_nifi_config.cli import main
from argus_nifi_config.session import Session
from argus_nifi_config.xmlconf import XmlConfig

AUTHORIZERS = """\
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<authorizers>
    <!-- keep this comment -->
    <userGroupProvider>
        <identifier>file-user-group-provider</identifier>
        <class>org.apache.nifi.authorization.FileUserGroupProvider</class>
        <property name="Users File">./conf/users.xml</property>
        <property name="Initial User Identity 1"></property>
    </userGroupProvider>
    <accessPolicyProvider>
        <identifier>file-access-policy-provider</identifier>
        <class>org.apache.nifi.authorization.FileAccessPolicyProvider</class>
        <property name="Initial Admin Identity"></property>
    </accessPolicyProvider>
    <authorizer>
        <identifier>managed-authorizer</identifier>
        <class>org.apache.nifi.authorization.StandardManagedAuthorizer</class>
    </authorizer>
</authorizers>
"""

STATE = """\
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<stateManagement>
    <cluster-provider>
        <id>zk-provider</id>
        <class>org.apache.nifi.controller.state.providers.zookeeper.ZooKeeperStateProvider</class>
        <property name="Connect String"></property>
        <property name="Root Node">/nifi</property>
    </cluster-provider>
</stateManagement>
"""

NIFI_PROPS = "nifi.web.https.port=8443\nnifi.cluster.is.node=false\n"


@pytest.fixture
def conf(tmp_path) -> Path:
    d = tmp_path / "conf"
    d.mkdir()
    (d / "authorizers.xml").write_text(AUTHORIZERS, encoding="utf-8")
    (d / "state-management.xml").write_text(STATE, encoding="utf-8")
    (d / "nifi.properties").write_text(NIFI_PROPS, encoding="utf-8")
    return d


def test_xml_set_property_preserves_comment(conf):
    xc = XmlConfig.load(conf / "authorizers.xml")
    xc.set_property("file-access-policy-provider", "Initial Admin Identity", "CN=admin")
    out = xc.dumps()
    assert "CN=admin" in out
    assert "keep this comment" in out  # 주석 보존
    assert out.startswith('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>')


def test_xml_providers_listed(conf):
    xc = XmlConfig.load(conf / "authorizers.xml")
    ids = {p.identifier for p in xc.providers()}
    assert {"file-user-group-provider", "file-access-policy-provider",
            "managed-authorizer"} <= ids


def test_cli_set_dry_run_does_not_write(conf):
    before = (conf / "nifi.properties").read_text()
    rc = main(["--conf-dir", str(conf), "--dry-run",
               "--set", "nifi.web.https.port=9443"])
    assert rc == 0
    assert (conf / "nifi.properties").read_text() == before  # 미변경


def test_cli_set_writes(conf):
    rc = main(["--conf-dir", str(conf), "--set", "nifi.web.https.port=9443"])
    assert rc == 0
    assert "nifi.web.https.port=9443" in (conf / "nifi.properties").read_text()


def test_cli_set_validation_rejects_bad_int(conf):
    with pytest.raises(SystemExit):
        main(["--conf-dir", str(conf), "--set", "nifi.web.https.port=notanint"])


def test_cli_set_out_dir_overlay(conf, tmp_path):
    out = tmp_path / "overlay"
    rc = main(["--conf-dir", str(conf), "--out", str(out),
               "--set", "nifi.web.https.port=9443"])
    assert rc == 0
    # 오버레이엔 편집된 파일만 생성
    assert (out / "nifi.properties").exists()
    assert not (out / "authorizers.xml").exists()


def test_cli_recipe_state_zookeeper(conf):
    rc = main(["--conf-dir", str(conf), "--recipe", "state:zookeeper",
               "--param", "connect_string=zk1:2181,zk2:2181"])
    assert rc == 0
    xml_out = (conf / "state-management.xml").read_text()
    assert "zk1:2181,zk2:2181" in xml_out
    props_out = (conf / "nifi.properties").read_text()
    assert "nifi.zookeeper.connect.string=zk1:2181,zk2:2181" in props_out
    assert "nifi.state.management.provider.cluster=zk-provider" in props_out


def test_cli_recipe_authorizers_file_admin(conf):
    rc = main(["--conf-dir", str(conf), "--recipe", "authorizers:file-admin",
               "--param", "admin_identity=CN=admin, OU=NiFi"])
    assert rc == 0
    xml_out = (conf / "authorizers.xml").read_text()
    assert "CN=admin, OU=NiFi" in xml_out
    assert "nifi.security.user.authorizer=managed-authorizer" in \
        (conf / "nifi.properties").read_text()


def test_session_touched_tracks_changes(conf):
    s = Session(conf)
    s.props("nifi.properties").set("nifi.web.https.port", "9443")
    assert "nifi.properties" in s.touched()
