#!/usr/bin/env bash
# Тестовые сертификаты для локального стенда Kafka с mTLS (имитация Aiven).
# Создаёт в local_kafka/certs/:
#   ca.pem, ca.key                — тестовый CA (аналог «CA certificate» Aiven)
#   service.cert, service.key     — клиентский сертификат и ключ (аналог Aiven)
#   broker.keystore.p12, broker.truststore.p12, broker_creds — для брокера
# Ничего общего с вашим Aiven: ключи только для локальной проверки, в git не попадают.
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p certs && cd certs
PASS="local-test-pass"
days=30

openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.pem -days $days \
  -subj "/CN=lab3-local-test-ca" 2>/dev/null

# брокер: SAN localhost, чтобы работала проверка имени хоста (ssl.endpoint.identification.algorithm=https)
openssl req -newkey rsa:2048 -nodes -keyout broker.key -out broker.csr -subj "/CN=localhost" 2>/dev/null
printf "subjectAltName=DNS:localhost,DNS:kafka,IP:127.0.0.1\n" > broker.ext
openssl x509 -req -in broker.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out broker.cert \
  -days $days -extfile broker.ext 2>/dev/null
openssl pkcs12 -export -inkey broker.key -in broker.cert -certfile ca.pem -name broker \
  -out broker.keystore.p12 -passout pass:$PASS
keytool -importcert -noprompt -alias ca -file ca.pem -keystore broker.truststore.p12 \
  -storetype PKCS12 -storepass $PASS >/dev/null
echo "$PASS" > broker_creds

# клиент (как service.cert / service.key у Aiven; ключ в PKCS#8 без пароля)
openssl req -newkey rsa:2048 -nodes -keyout service.key -out service.csr -subj "/CN=lab3-client" 2>/dev/null
openssl x509 -req -in service.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out service.cert -days $days 2>/dev/null
rm -f *.csr *.ext *.srl
chmod 644 ./*                 # брокер в контейнере читает файлы от своего пользователя
chmod 600 service.key ca.key
echo "Сертификаты созданы в $(pwd)"
