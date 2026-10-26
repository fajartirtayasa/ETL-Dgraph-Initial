#!/bin/bash
# ======================================================
# Dgraph Zero Setup Script for Ubuntu/Amazon Linux
# ======================================================

set -e

echo "===> Updating system..."
sudo apt update -y || sudo yum update -y
sudo apt install -y wget tar || sudo yum install -y wget tar

echo "===> Setting system-wide file descriptor limits..."
sudo tee -a /etc/systemd/system.conf >/dev/null <<'EOF'
DefaultLimitNOFILE=2000000
DefaultLimitNPROC=2000000
DefaultLimitMEMLOCK=2000000
EOF

sudo tee -a /etc/security/limits.conf >/dev/null <<'EOF'
* soft nofile 2000000
* hard nofile 2000000
root soft nofile 2000000
root hard nofile 2000000
EOF

# Ensure PAM applies limits
if ! grep -q pam_limits.so /etc/pam.d/common-session 2>/dev/null; then
  echo "session required pam_limits.so" | sudo tee -a /etc/pam.d/common-session
fi

if [ -f /etc/pam.d/common-session-noninteractive ]; then
  if ! grep -q pam_limits.so /etc/pam.d/common-session-noninteractive; then
    echo "session required pam_limits.so" | sudo tee -a /etc/pam.d/common-session-noninteractive
  fi
fi

# systl
sudo tee /etc/sysctl.d/99-dgraph-zero.conf > /dev/null <<'EOF'
# Dgraph Zero tuned sysctl (c5d.large)
net.core.somaxconn = 65535
net.core.netdev_max_backlog = 16384

net.core.rmem_max = 268435456
net.core.wmem_max = 268435456
net.core.rmem_default = 134217728
net.core.wmem_default = 134217728

net.ipv4.tcp_rmem = 4096 87380 268435456
net.ipv4.tcp_wmem = 4096 65536 268435456
net.ipv4.tcp_fastopen = 3
net.ipv4.tcp_fin_timeout = 15
net.ipv4.tcp_tw_reuse = 1
net.core.default_qdisc = fq
net.ipv4.tcp_congestion_control = bbr

fs.file-max = 2000000
fs.nr_open = 2000000

vm.dirty_ratio = 10
vm.dirty_background_ratio = 5
vm.dirty_expire_centisecs = 3000
vm.dirty_writeback_centisecs = 500
vm.swappiness = 1
vm.overcommit_memory = 1
vm.nr_hugepages = 0

net.ipv4.ip_local_port_range = 1024 65535
net.ipv6.conf.all.disable_ipv6 = 1
net.ipv6.conf.default.disable_ipv6 = 1
EOF

sudo sysctl --system

echo "===> Reloading systemd..."
sudo systemctl daemon-reexec
sudo systemctl daemon-reload

echo "===> Installing Dgraph..."
wget -q https://github.com/dgraph-io/dgraph/releases/latest/download/dgraph-linux-amd64.tar.gz
tar -xvzf dgraph-linux-amd64.tar.gz
sudo mv dgraph /usr/local/bin/
dgraph version

echo "===> Creating data directory..."
sudo mkdir -p /data/zero
sudo mkdir -p /data/zero/{w,zw}
sudo chown -R $(whoami):$(whoami) /data/zero
sudo chmod -R 755 /data/zero

PRIVATE_IP=$(hostname -I | awk '{print $1}')

echo "===> Creating systemd service for Dgraph Zero..."
sudo tee /etc/systemd/system/dgraph-zero.service >/dev/null <<EOF
[Unit]
Description=Dgraph Zero Service
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/data/zero
ExecStart=/usr/local/bin/dgraph zero \\
  --my=${PRIVATE_IP}:5080 \\
  --replicas=3 \\
  --bindall=true \\
  --telemetry="reports=false; sentry=false" \\
  --wal="/data/zero/w" \\
  --peer 172.30.0.65:5080 \\
  --raft="idx=3" \\
  --v=2
Restart=always
RestartSec=3
LimitNOFILE=2000000
LimitNPROC=2000000
LimitCORE=2000000

[Install]
WantedBy=multi-user.target
EOF

echo "===> Enabling and starting Dgraph Zero..."
sudo systemctl daemon-reload
sudo systemctl enable dgraph-zero
sudo systemctl start dgraph-zero
sudo systemctl status dgraph-zero --no-pager
sudo journalctl -u dgraph-zero -n 50

echo "===> Setup complete!"
echo "Check Zero state:  curl http://${PRIVATE_IP}:6080/state"
