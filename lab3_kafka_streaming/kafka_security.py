"""SSL/SASL-подключение к Aiven for Apache Kafka для Python-клиента и Spark (JVM).

Способы аутентификации Aiven:
  * certificate — взаимный TLS (mTLS): ca.pem + service.cert + service.key;
  * sasl        — SASL_SSL с SCRAM-SHA-256 (логин/пароль) + ca.pem.

Python-клиент (confluent-kafka / librdkafka) читает PEM-файлы напрямую.
Spark Kafka connector работает в JVM (kafka-clients 3.9.1). Для неё из PEM
создаются:
  * truststore (PKCS12) с CA-сертификатом — keytool;
  * keystore (PKCS12) с клиентским сертификатом и приватным ключом — openssl.
Пароли хранилищ генерируются случайно на сессию и передаются утилитам через
переменные окружения, а не в командной строке. Содержимое ключей не выводится.

Секреты кладутся только в secrets_dir на диске ВМ (по умолчанию
/content/kafka_secrets), но не в Google Drive.
"""
from __future__ import annotations

import os
import re
import secrets as pysecrets
import shutil
import stat
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

CA_FILE, CERT_FILE, KEY_FILE = "ca.pem", "service.cert", "service.key"
TRUSTSTORE, KEYSTORE = "client.truststore.p12", "client.keystore.p12"


class KafkaSecurityError(RuntimeError):
    """Понятная ошибка настройки безопасности (без секретов в тексте)."""


@dataclass
class KafkaConnection:
    bootstrap_servers: str
    auth_mode: str = "certificate"          # certificate | sasl
    secrets_dir: Path = Path("/content/kafka_secrets")
    sasl_username: str | None = None
    sasl_password: str | None = field(default=None, repr=False)
    sasl_mechanism: str = "SCRAM-SHA-256"
    store_password: str = field(default_factory=lambda: pysecrets.token_urlsafe(24), repr=False)

    # ---- пути
    @property
    def ca(self) -> Path:
        return Path(self.secrets_dir) / CA_FILE

    @property
    def cert(self) -> Path:
        return Path(self.secrets_dir) / CERT_FILE

    @property
    def key(self) -> Path:
        return Path(self.secrets_dir) / KEY_FILE

    @property
    def truststore(self) -> Path:
        return Path(self.secrets_dir) / TRUSTSTORE

    @property
    def keystore(self) -> Path:
        return Path(self.secrets_dir) / KEYSTORE

    def __post_init__(self):
        self.secrets_dir = Path(self.secrets_dir)
        self.bootstrap_servers = (self.bootstrap_servers or "").strip()
        if self.auth_mode not in ("certificate", "sasl"):
            raise KafkaSecurityError(f"auth_mode должен быть certificate или sasl, получено {self.auth_mode!r}")
        if not re.fullmatch(r"[A-Za-z0-9.\-]+:\d{1,5}(,[A-Za-z0-9.\-]+:\d{1,5})*", self.bootstrap_servers):
            raise KafkaSecurityError(
                "bootstrap_servers должен иметь вид host:port (Aiven: <service>-<project>.<...>.aivencloud.com:<port>)")

    # ---- конфигурации клиентов
    def confluent_config(self, **extra) -> dict:
        """Параметры для confluent_kafka.Producer / Consumer / AdminClient."""
        conf = {"bootstrap.servers": self.bootstrap_servers, "ssl.ca.location": str(self.ca),
                "client.id": "lab3-colab"}
        if self.auth_mode == "certificate":
            conf.update({"security.protocol": "SSL",
                         "ssl.certificate.location": str(self.cert),
                         "ssl.key.location": str(self.key)})
        else:
            conf.update({"security.protocol": "SASL_SSL", "sasl.mechanism": self.sasl_mechanism,
                         "sasl.username": self.sasl_username, "sasl.password": self.sasl_password})
        conf.update(extra)
        return conf

    def spark_options(self) -> dict:
        """Опции для spark.readStream.format("kafka"): префикс kafka. передаёт их kafka-clients."""
        opts = {
            "kafka.bootstrap.servers": self.bootstrap_servers,
            "kafka.ssl.truststore.location": str(self.truststore),
            "kafka.ssl.truststore.type": "PKCS12",
            "kafka.ssl.truststore.password": self.store_password,
            # проверка имени хоста включена по умолчанию в kafka-clients >= 2.0; задаём явно
            "kafka.ssl.endpoint.identification.algorithm": "https",
        }
        if self.auth_mode == "certificate":
            opts.update({
                "kafka.security.protocol": "SSL",
                "kafka.ssl.keystore.location": str(self.keystore),
                "kafka.ssl.keystore.type": "PKCS12",
                "kafka.ssl.keystore.password": self.store_password,
                "kafka.ssl.key.password": self.store_password,
            })
        else:
            user = self.sasl_username.replace('"', '\\"')
            pwd = self.sasl_password.replace('"', '\\"')
            opts.update({
                "kafka.security.protocol": "SASL_SSL",
                "kafka.sasl.mechanism": self.sasl_mechanism,
                "kafka.sasl.jaas.config":
                    f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{user}" password="{pwd}";',
            })
        return opts

    def describe(self) -> dict:
        """Безопасное описание конфигурации для журнала (без паролей и ключей)."""
        host = self.bootstrap_servers.split(",")[0]
        masked = re.sub(r"^[^.]+", lambda m: m.group(0)[:4] + "***", host)
        return {"bootstrap": masked, "auth_mode": self.auth_mode, "secrets_dir": str(self.secrets_dir),
                "truststore": self.truststore.name, "keystore": self.keystore.name if self.auth_mode == "certificate" else None}


# ---------------------------------------------------------------------------
# Размещение секретов
# ---------------------------------------------------------------------------
def prepare_secrets_dir(path: Path | str) -> Path:
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    os.chmod(path, stat.S_IRWXU)  # 700
    if "/drive/" in str(path).lower():
        raise KafkaSecurityError("Каталог секретов не должен находиться в Google Drive")
    return path


_PEM_BLOCK = re.compile(r"-----BEGIN ([A-Z0-9 ]+)-----(.*?)-----END \1-----", re.S)


def normalize_pem(text: str) -> str:
    """Восстановить переносы строк PEM, если значение вставлено одной строкой
    (например, в поле «Секреты» Colab). Корректный PEM не меняется."""
    blocks = _PEM_BLOCK.findall(text)
    if not blocks:
        return text
    out = []
    for label, body in blocks:
        b64 = re.sub(r"\s+", "", body)
        lines = [b64[i:i + 64] for i in range(0, len(b64), 64)]
        out.append("\n".join([f"-----BEGIN {label}-----", *lines, f"-----END {label}-----"]))
    return "\n".join(out) + "\n"


def write_secret_file(path: Path, content: str | bytes):
    """Записать секрет с правами 600 (без вывода содержимого)."""
    data = content.encode("utf-8") if isinstance(content, str) else content
    data = data.replace(b"\r\n", b"\n").strip() + b"\n"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as fh:
        fh.write(data)
    os.chmod(path, 0o600)


def install_from_files(conn: KafkaConnection, ca: Path | str, cert: Path | str | None = None,
                       key: Path | str | None = None):
    """Скопировать скачанные из Aiven файлы в secrets_dir под стандартными именами."""
    prepare_secrets_dir(conn.secrets_dir)
    pairs = [(ca, conn.ca)]
    if conn.auth_mode == "certificate":
        pairs += [(cert, conn.cert), (key, conn.key)]
    for src, dst in pairs:
        if src is None or not Path(src).is_file():
            raise KafkaSecurityError(f"Не найден файл {dst.name}: {src}")
        if Path(src).resolve() != dst.resolve():
            write_secret_file(dst, Path(src).read_bytes())


# ---------------------------------------------------------------------------
# Проверка PEM и создание хранилищ
# ---------------------------------------------------------------------------
def _run(cmd: list[str], env_extra: dict | None = None, what: str = "") -> str:
    exe = cmd[0]
    if shutil.which(exe) is None:
        raise KafkaSecurityError(f"Не найдена утилита {exe}. Colab: входит в образ; Arch Linux: "
                                 f"{'openssl' if exe == 'openssl' else 'jdk21-openjdk'} (pacman -S)")
    env = dict(os.environ)
    env.update(env_extra or {})
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if proc.returncode != 0:
        # stderr OpenSSL/keytool не содержит ключей; обрезаем на всякий случай
        msg = (proc.stderr or proc.stdout).strip().splitlines()
        tail = " | ".join(msg[-3:])[:400]
        raise KafkaSecurityError(f"{what or exe} завершилась с ошибкой {proc.returncode}: {tail}")
    return proc.stdout


def _check_pem(path: Path, markers: tuple[str, ...], label: str):
    if not path.is_file() or path.stat().st_size == 0:
        raise KafkaSecurityError(f"{label}: файл {path.name} отсутствует или пуст")
    head = path.read_text(encoding="utf-8", errors="replace")
    if not any(f"-----BEGIN {m}-----" in head for m in markers):
        raise KafkaSecurityError(f"{label}: {path.name} не похож на PEM ({' / '.join(markers)})")


def validate_pem_files(conn: KafkaConnection) -> dict:
    """Проверить наличие, формат, срок действия и соответствие ключа сертификату.

    Возвращает только открытые сведения: subject, issuer, срок действия.
    """
    info = {}
    _check_pem(conn.ca, ("CERTIFICATE",), "CA certificate")
    info["ca"] = _cert_info(conn.ca)
    if conn.auth_mode == "certificate":
        _check_pem(conn.cert, ("CERTIFICATE",), "Service certificate")
        _check_pem(conn.key, ("PRIVATE KEY", "RSA PRIVATE KEY", "EC PRIVATE KEY"), "Service key")
        info["service_cert"] = _cert_info(conn.cert)
        cert_pub = _run(["openssl", "x509", "-in", str(conn.cert), "-noout", "-pubkey"], what="openssl x509 -pubkey")
        key_pub = _run(["openssl", "pkey", "-in", str(conn.key), "-pubout"], what="openssl pkey -pubout")
        if cert_pub.strip() != key_pub.strip():
            raise KafkaSecurityError("service.key не соответствует service.cert (разные открытые ключи)")
        info["key_matches_cert"] = True
        _run(["openssl", "verify", "-CAfile", str(conn.ca), str(conn.cert)], what="openssl verify")
        info["cert_signed_by_ca"] = True
    for k in ("ca", "service_cert"):
        if k in info and info[k]["expired"]:
            raise KafkaSecurityError(f"Сертификат {k} просрочен: {info[k]['not_after']}")
    return info


def _cert_info(path: Path) -> dict:
    out = _run(["openssl", "x509", "-in", str(path), "-noout", "-subject", "-issuer", "-enddate"],
               what=f"openssl x509 ({path.name})")
    fields = dict(line.split("=", 1) for line in out.strip().splitlines() if "=" in line)
    not_after = fields.get("notAfter", "").strip()
    expired = False
    try:
        dt = datetime.strptime(not_after, "%b %d %H:%M:%S %Y %Z").replace(tzinfo=timezone.utc)
        expired = dt < datetime.now(timezone.utc)
    except ValueError:
        pass
    return {"subject": fields.get("subject", "").strip(), "issuer": fields.get("issuer", "").strip(),
            "not_after": not_after, "expired": expired}


def build_java_stores(conn: KafkaConnection) -> dict:
    """Создать truststore и (для certificate) keystore в формате PKCS12 для JVM Spark.

    Повторный вызов пересоздаёт хранилища с текущим паролем сессии.
    """
    prepare_secrets_dir(conn.secrets_dir)
    env = {"LAB3_STORE_PASS": conn.store_password}
    for p in (conn.truststore, conn.keystore):
        if p.exists():
            p.unlink()
    _run(["keytool", "-importcert", "-noprompt", "-alias", "aiven-ca", "-file", str(conn.ca),
          "-keystore", str(conn.truststore), "-storetype", "PKCS12", "-storepass:env", "LAB3_STORE_PASS"],
         env, "keytool -importcert (truststore)")
    os.chmod(conn.truststore, 0o600)
    result = {"truststore": str(conn.truststore)}
    if conn.auth_mode == "certificate":
        _run(["openssl", "pkcs12", "-export", "-inkey", str(conn.key), "-in", str(conn.cert),
              "-certfile", str(conn.ca), "-name", "service_key", "-out", str(conn.keystore),
              "-passout", "env:LAB3_STORE_PASS"], env, "openssl pkcs12 -export (keystore)")
        os.chmod(conn.keystore, 0o600)
        listing = _run(["keytool", "-list", "-keystore", str(conn.keystore), "-storetype", "PKCS12",
                        "-storepass:env", "LAB3_STORE_PASS"], env, "keytool -list (keystore)")
        if "PrivateKeyEntry" not in listing:
            raise KafkaSecurityError("keystore создан, но не содержит PrivateKeyEntry")
        result["keystore"] = str(conn.keystore)
    listing = _run(["keytool", "-list", "-keystore", str(conn.truststore), "-storetype", "PKCS12",
                    "-storepass:env", "LAB3_STORE_PASS"], env, "keytool -list (truststore)")
    if "trustedCertEntry" not in listing:
        raise KafkaSecurityError("truststore создан, но не содержит trustedCertEntry")
    return result


def load_from_colab(conn: KafkaConnection, logger=None) -> str:
    """Получить PEM-файлы в Colab: из «Секретов» Colab, иначе через окно загрузки файлов.

    Секреты Colab (значок ключа слева): KAFKA_CA_PEM, KAFKA_SERVICE_CERT, KAFKA_SERVICE_KEY
    (содержимое файлов целиком). Если их нет — откроется диалог загрузки трёх файлов.
    """
    prepare_secrets_dir(conn.secrets_dir)
    needed = {CA_FILE: "KAFKA_CA_PEM"}
    if conn.auth_mode == "certificate":
        needed.update({CERT_FILE: "KAFKA_SERVICE_CERT", KEY_FILE: "KAFKA_SERVICE_KEY"})
    have = {name: (conn.secrets_dir / name).is_file() for name in needed}
    if all(have.values()):
        return "already-present"
    try:
        from google.colab import userdata  # type: ignore
        got = 0
        for name, secret_name in needed.items():
            try:
                value = userdata.get(secret_name)
            except Exception:
                value = None
            if value:
                write_secret_file(conn.secrets_dir / name, normalize_pem(value))
                got += 1
        if got == len(needed):
            return "colab-secrets"
    except ImportError:
        raise KafkaSecurityError("Не Colab: положите файлы в secrets_dir или используйте install_from_files()")
    from google.colab import files  # type: ignore
    print("Выберите файлы, скачанные из Aiven:", ", ".join(needed))
    uploaded = files.upload()
    for fname, content in uploaded.items():
        target = _match_uploaded(fname, list(needed))
        if target:
            write_secret_file(conn.secrets_dir / target, content)
        # загруженное также оказывается в текущем каталоге — удаляем копию
        try:
            os.remove(fname)
        except OSError:
            pass
    missing = [n for n in needed if not (conn.secrets_dir / n).is_file()]
    if missing:
        raise KafkaSecurityError(f"Не загружены файлы: {', '.join(missing)}")
    return "upload"


def _match_uploaded(fname: str, needed: list[str]) -> str | None:
    low = fname.lower()
    if "ca" in low and low.endswith(".pem"):
        return CA_FILE if CA_FILE in needed else None
    if low.endswith(".cert") or ("service" in low and low.endswith((".crt", ".pem"))):
        return CERT_FILE if CERT_FILE in needed else None
    if low.endswith(".key"):
        return KEY_FILE if KEY_FILE in needed else None
    return None
