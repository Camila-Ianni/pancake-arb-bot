#!/bin/bash
set -euo pipefail

sudo mkdir -p /var/log/polymarket-arb/
sudo chown -R "$USER:$USER" /var/log/polymarket-arb/

cd go
GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o sniper cmd/sniper/main.go
cd ..

cat << EOF | sudo tee /etc/systemd/system/sniper.service
[Unit]
Description=Polymarket Multi-Asset HFT Sniper Bot
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$(pwd)/go
ExecStart=$(pwd)/go/sniper
Restart=always
RestartSec=5
StandardOutput=append:/var/log/polymarket-arb/sniper.log
StandardError=append:/var/log/polymarket-arb/sniper_error.log
Environment=GOGC=1600
Environment=GOMEMLIMIT=256MiB

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable sniper.service --now
