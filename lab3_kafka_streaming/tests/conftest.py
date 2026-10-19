"""Общие фикстуры тестов ЛР № 3."""
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _java_major():
    if shutil.which("java") is None:
        return 0
    p = subprocess.run(["java", "-version"], capture_output=True, text=True)
    for line in (p.stderr + p.stdout).splitlines():
        if 'version "' in line:
            v = line.split('"')[1]
            return int(v.split(".")[0])
    return 0


def spark_available():
    try:
        import pyspark  # noqa: F401
    except ImportError:
        return False, "PySpark не установлен (pip install -r requirements-spark.txt)"
    if _java_major() < 17:
        return False, "нужна Java 17+ (Arch: sudo pacman -S jdk21-openjdk)"
    return True, ""


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    ok, why = spark_available()
    if not ok:
        pytest.skip(why)
    import lab3_common as C
    import spark_jobs as J
    work = tmp_path_factory.mktemp("spark_work")
    s = C.Settings(base_dir=str(work / "base"), work_dir=str(work / "work"), shuffle_partitions=2)
    session = J.get_spark(s, app_name="lab3-tests", with_kafka=bool(os.environ.get("LAB3_IT_BOOTSTRAP")),
                          master="local[2]", driver_memory="1g")
    yield session
    session.stop()


@pytest.fixture
def settings(tmp_path):
    import lab3_common as C
    s = C.Settings(base_dir=str(tmp_path / "base"), work_dir=str(tmp_path / "work"),
                   secrets_dir=str(tmp_path / "secrets"), shuffle_partitions=2)
    s.ensure_dirs()
    return s


@pytest.fixture(scope="session")
def test_pki(tmp_path_factory):
    """Тестовые CA, клиентский сертификат и «чужой» ключ (OpenSSL)."""
    if shutil.which("openssl") is None:
        pytest.skip("нет openssl")
    d = tmp_path_factory.mktemp("pki")

    def run(*cmd):
        subprocess.run(cmd, check=True, capture_output=True, cwd=d)

    run("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", "ca.key", "-out", "ca.pem",
        "-days", "2", "-subj", "/CN=test-ca")
    run("openssl", "req", "-newkey", "rsa:2048", "-nodes", "-keyout", "service.key", "-out", "service.csr",
        "-subj", "/CN=test-client")
    run("openssl", "x509", "-req", "-in", "service.csr", "-CA", "ca.pem", "-CAkey", "ca.key",
        "-CAcreateserial", "-out", "service.cert", "-days", "2")
    run("openssl", "genpkey", "-algorithm", "RSA", "-out", "other.key")
    return d
