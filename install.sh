#!/usr/bin/env bash

set -eo pipefail

GO_VERSION="1.26.3"
GO_ARCHIVE="go${GO_VERSION}.linux-amd64.tar.gz"
GO_URL="https://go.dev/dl/${GO_ARCHIVE}"

if [[ $EUID -ne 0 ]]; then
    echo "Ошибка: запустите скрипт от root."
    exit 1
fi

echo "======================================"
echo "Установка зависимостей"
echo "======================================"

apt update
apt install -y git wget curl build-essential

echo
echo "======================================"
echo "Установка Go ${GO_VERSION}"
echo "======================================"

cd /tmp
wget -O "${GO_ARCHIVE}" "${GO_URL}"

rm -rf /usr/local/go
tar -C /usr/local -xzf "${GO_ARCHIVE}"

export PATH=$PATH:/usr/local/go/bin

if ! grep -q "/usr/local/go/bin" /root/.bashrc; then
    echo 'export PATH=$PATH:/usr/local/go/bin' >> /root/.bashrc
fi

if ! grep -q '$HOME/go/bin' /root/.bashrc; then
    echo 'export PATH="$HOME/go/bin:$PATH"' >> /root/.bashrc
fi

export PATH=$PATH:/root/go/bin

echo
echo "======================================"
echo "Установка Mage"
echo "======================================"

go install github.com/magefile/mage@latest

echo
echo "======================================"
echo "Клонирование репозитория"
echo "======================================"

cd /root

if [[ ! -d olcrtc ]]; then
    git clone https://github.com/openlibrecommunity/olcrtc --recurse-submodules
fi

cd olcrtc

echo
echo "======================================"
echo "Создание swap (если отсутствует)"
echo "======================================"

if ! swapon --show | grep -q "/swapfile"; then

    if [[ ! -f /swapfile ]]; then

        if ! fallocate -l 2G /swapfile; then
            dd if=/dev/zero of=/swapfile bs=1M count=2048
        fi

        chmod 600 /swapfile
        mkswap /swapfile
    fi

    swapon /swapfile

    if ! grep -q "^/swapfile" /etc/fstab; then
        echo "/swapfile none swap sw 0 0" >> /etc/fstab
    fi
fi

echo
echo "======================================"
echo "Сборка проекта"
echo "======================================"

mage build

if [[ ! -f build/olcrtc-linux-amd64 ]]; then
    echo "Ошибка: сборка завершилась неудачно."
    exit 1
fi

echo
echo "======================================"
echo "Настройка сервера"
echo "======================================"

exec 3</dev/tty

ROOM_ID=""

while [[ -z "$ROOM_ID" ]]; do
    printf "Введите URL комнаты Jitsi: " >/dev/tty
    IFS= read -r ROOM_ID <&3
done

CRYPTO_KEY=""

while [[ -z "$CRYPTO_KEY" ]]; do
    printf "Введите ключ шифрования: " >/dev/tty
    IFS= read -rs CRYPTO_KEY <&3
    echo >/dev/tty
done

cat > server.yaml <<'EOF'
mode: srv

auth:
  provider: jitsi

room:
  id: "ROOM_ID"

crypto:
  key: "CRYPTO_KEY"

net:
  transport: datachannel
  dns: "8.8.8.8:53"

liveness:
  interval: 10s
  timeout: 5s
  failures: 3

data: data
debug: false
EOF

sed -i "s|ROOM_ID|${ROOM_ID}|g" server.yaml
sed -i "s|CRYPTO_KEY|${CRYPTO_KEY}|g" server.yaml

echo
echo "======================================"
echo "Установка бинарника"
echo "======================================"

mkdir -p /opt/olcrtc

cp ./build/olcrtc-linux-amd64 /opt/olcrtc/
cp ./server.yaml /opt/olcrtc/

rm -f ./server.yaml

echo
echo "======================================"
echo "Создание systemd сервиса"
echo "======================================"

cat > /etc/systemd/system/olcrtc.service <<EOF
[Unit]
Description=OlcRTC Proxy Server
After=network.target network-online.target
StartLimitIntervalSec=0

[Service]
Type=simple
WorkingDirectory=/opt/olcrtc
ExecStart=/opt/olcrtc/olcrtc-linux-amd64 server.yaml
Restart=always
RestartSec=5
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
EOF

if [[ ! -f ./build/olcrtc-linux-amd64 ]]; then
    echo "Ошибка: бинарный файл ./build/olcrtc-linux-amd64 не найден."
    exit 1
fi

systemctl daemon-reload
systemctl enable olcrtc.service
systemctl start olcrtc.service

echo
echo "======================================"
echo "Установка успешно завершена!"
echo "======================================"
echo
systemctl --no-pager --full status olcrtc.service || true
