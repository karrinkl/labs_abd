"""Общие функции и настройка SSL (OpenSSL/keytool) без сети."""
import logging
import os
import shutil
import stat

import pytest

import kafka_security as K
import lab3_common as C


# ---------------------------------------------------------------- lab3_common
def test_write_csv_atomic_overwrites_and_is_readable(tmp_path):
    p = tmp_path / "r.csv"
    C.write_csv_atomic(p, [{"a": 1, "b": 2}])
    C.write_csv_atomic(p, [{"a": 3, "b": 4}, {"a": 5, "b": 6}])
    assert C.read_csv(p) == [{"a": "3", "b": "4"}, {"a": "5", "b": "6"}]
    assert not list(tmp_path.glob("*.tmp"))
    assert stat.S_IMODE(p.stat().st_mode) == 0o644


def test_compare_dimension_stats_computes_deltas():
    before = [{"dimension": "total", "value": "all", "tx_count": "10", "amount_eur_sum": "100"}]
    after = [{"dimension": "total", "value": "all", "tx_count": "13", "amount_eur_sum": "160"},
             {"dimension": "city", "value": "Tomsk", "tx_count": "3", "amount_eur_sum": "60"}]
    out = {(r["dimension"], r["value"]): r for r in C.compare_dimension_stats(before, after)}
    assert out[("total", "all")]["tx_count_delta"] == 3
    assert out[("total", "all")]["amount_eur_sum_delta"] == 60
    assert out[("city", "Tomsk")]["tx_count_before"] == 0 and out[("city", "Tomsk")]["avg_amount_eur_before"] is None


def test_setup_logging_is_idempotent(tmp_path):
    log = C.setup_logging(tmp_path / "log.txt")
    log = C.setup_logging(tmp_path / "log.txt")
    log.info("hello")
    assert len([h for h in log.handlers if getattr(h, "_lab3", None) == "file"]) == 1
    assert "hello" in (tmp_path / "log.txt").read_text(encoding="utf-8")
    for h in list(log.handlers):
        log.removeHandler(h)


# ---------------------------------------------------------------- конфигурация подключения
def test_bootstrap_format_is_validated(tmp_path):
    with pytest.raises(K.KafkaSecurityError):
        K.KafkaConnection("https://not-a-host", secrets_dir=tmp_path)
    with pytest.raises(K.KafkaSecurityError):
        K.KafkaConnection("host:123", auth_mode="plaintext", secrets_dir=tmp_path)


def test_secrets_never_in_repr_or_describe(tmp_path):
    c = K.KafkaConnection("kafka-x.aivencloud.com:12345", "sasl", tmp_path, "avnadmin", "s3cr3t-pass")
    assert "s3cr3t-pass" not in repr(c) and c.store_password not in repr(c)
    assert "s3cr3t-pass" not in str(c.describe()) and c.store_password not in str(c.describe())
    assert c.describe()["bootstrap"].startswith("kafk***")


def test_client_configs_for_certificate_and_sasl(tmp_path):
    cert = K.KafkaConnection("h.example.com:1", "certificate", tmp_path)
    cc = cert.confluent_config()
    assert cc["security.protocol"] == "SSL" and cc["ssl.key.location"].endswith("service.key")
    so = cert.spark_options()
    for key in ("kafka.security.protocol", "kafka.ssl.truststore.location", "kafka.ssl.keystore.location",
                "kafka.ssl.keystore.type", "kafka.ssl.key.password"):
        assert key in so
    assert so["kafka.ssl.keystore.type"] == "PKCS12"
    sasl = K.KafkaConnection("h.example.com:2", "sasl", tmp_path, 'u"ser', "p")
    so = sasl.spark_options()
    assert so["kafka.security.protocol"] == "SASL_SSL" and "kafka.ssl.keystore.location" not in so
    assert 'username="u\\"ser"' in so["kafka.sasl.jaas.config"]
    assert sasl.confluent_config()["sasl.mechanism"] == "SCRAM-SHA-256"


def test_secrets_dir_in_google_drive_is_rejected(tmp_path):
    with pytest.raises(K.KafkaSecurityError):
        K.prepare_secrets_dir(tmp_path / "drive" / "MyDrive" / "keys")


# ---------------------------------------------------------------- PEM и хранилища
needs_keytool = pytest.mark.skipif(shutil.which("keytool") is None, reason="нет keytool (Arch: jdk21-openjdk)")


def _conn(tmp_path, pki, key="service.key"):
    c = K.KafkaConnection("localhost:9093", "certificate", tmp_path / "secrets")
    K.install_from_files(c, pki / "ca.pem", pki / "service.cert", pki / key)
    return c


def test_validate_pem_accepts_matching_pair(tmp_path, test_pki):
    info = K.validate_pem_files(_conn(tmp_path, test_pki))
    assert info["key_matches_cert"] and info["cert_signed_by_ca"]
    assert "test-client" in info["service_cert"]["subject"] and not info["ca"]["expired"]
    assert stat.S_IMODE((tmp_path / "secrets" / "service.key").stat().st_mode) == 0o600


def test_validate_pem_rejects_foreign_key(tmp_path, test_pki):
    with pytest.raises(K.KafkaSecurityError, match="не соответствует"):
        K.validate_pem_files(_conn(tmp_path, test_pki, key="other.key"))


def test_validate_pem_rejects_missing_and_non_pem(tmp_path, test_pki):
    c = K.KafkaConnection("localhost:9093", "certificate", tmp_path / "s")
    with pytest.raises(K.KafkaSecurityError):
        K.install_from_files(c, test_pki / "ca.pem", test_pki / "nope.cert", test_pki / "service.key")
    K.prepare_secrets_dir(c.secrets_dir)
    K.write_secret_file(c.ca, "not a certificate")
    with pytest.raises(K.KafkaSecurityError, match="PEM"):
        K.validate_pem_files(c)


@needs_keytool
def test_build_java_stores_creates_pkcs12_with_entries(tmp_path, test_pki):
    c = _conn(tmp_path, test_pki)
    res = K.build_java_stores(c)
    assert os.path.getsize(res["truststore"]) > 0 and os.path.getsize(res["keystore"]) > 0
    assert stat.S_IMODE(c.keystore.stat().st_mode) == 0o600
    res2 = K.build_java_stores(c)  # повторный запуск не падает
    assert res2 == res


@needs_keytool
def test_store_errors_are_readable_and_secret_free(tmp_path, test_pki):
    c = _conn(tmp_path, test_pki)
    K.write_secret_file(c.key, "-----BEGIN PRIVATE KEY-----\nbroken\n-----END PRIVATE KEY-----\n")
    with pytest.raises(K.KafkaSecurityError) as e:
        K.build_java_stores(c)
    assert "openssl" in str(e.value) and c.store_password not in str(e.value)


def test_logger_does_not_leak_store_password(tmp_path, caplog):
    c = K.KafkaConnection("localhost:9093", "certificate", tmp_path)
    with caplog.at_level(logging.INFO, logger="lab3"):
        logging.getLogger("lab3").info("cfg %s", c.describe())
    assert c.store_password not in caplog.text


def test_normalize_pem_restores_single_line_paste(test_pki):
    pem = (test_pki / "service.cert").read_text()
    flat = pem.replace("\n", " ")
    assert K.normalize_pem(flat).strip() == pem.strip()
    assert K.normalize_pem(pem).strip() == pem.strip()
    assert K.normalize_pem("no pem here") == "no pem here"
