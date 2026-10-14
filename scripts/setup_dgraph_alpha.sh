#!/bin/bash
# ======================================================
# Dgraph Alpha Setup Script for Ubuntu/Amazon Linux
# ======================================================

set -e

if [ -z "$1" ]; then
  echo "Usage: ./setup_dgraph_alpha.sh <ZERO_PRIVATE_IP>"
  exit 1
fi

ZERO_IP=$1

echo "===> Updating system..."
sudo apt update -y || sudo yum update -y
sudo apt install -y wget tar || sudo yum install -y wget tar

echo "===> Setting system-wide file descriptor limits..."
sudo tee -a /etc/systemd/system.conf >/dev/null <<'EOF'
DefaultLimitNOFILE=33554432
DefaultLimitNPROC=33554432
DefaultLimitMEMLOCK=33554432
EOF

sudo tee -a /etc/security/limits.conf >/dev/null <<'EOF'
* soft nofile 33554432
* hard nofile 33554432
root soft nofile 33554432
root hard nofile 33554432
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
sudo tee /etc/sysctl.d/99-dgraph-alpha.conf > /dev/null <<'EOF'
# Dgraph Alpha tuned sysctl (c5d.4xlarge)
fs.nr_open = 33554432
fs.file-max = 33554432
fs.inotify.max_user_watches = 524288
fs.inotify.max_user_instances = 1024

vm.swappiness = 1
vm.overcommit_memory = 0
vm.dirty_background_bytes = 268435456
vm.dirty_bytes = 1073741824
vm.dirty_expire_centisecs = 3000
vm.dirty_writeback_centisecs = 500
vm.max_map_count = 262144
vm.page-cluster = 3

net.core.somaxconn = 65535
net.core.netdev_max_backlog = 65536
net.core.rmem_max = 268435456
net.core.wmem_max = 268435456
net.core.rmem_default = 262144
net.core.wmem_default = 262144
net.core.default_qdisc = fq
net.ipv4.tcp_congestion_control = bbr

net.ipv4.tcp_keepalive_time = 60
net.ipv4.tcp_keepalive_intvl = 10
net.ipv4.tcp_keepalive_probes = 6
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_fin_timeout = 10
net.ipv4.tcp_rmem = 4096 87380 268435456
net.ipv4.tcp_wmem = 4096 65536 268435456

net.netfilter.nf_conntrack_max = 262144
net.netfilter.nf_conntrack_buckets = 65536

kernel.sched_autogroup_enabled = 1
kernel.pid_max = 4194303

block.nr_requests = 1024
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

echo "===> Creating data directories..."
sudo mkdir -p /data/alpha/{p,w,export}
sudo chown -R $(whoami):$(whoami) /data/alpha
sudo chmod -R 755 /data/alpha

PRIVATE_IP=$(hostname -I | awk '{print $1}')

echo "===> Creating systemd service for Dgraph Alpha..."
sudo tee /etc/systemd/system/dgraph-alpha.service >/dev/null <<EOF
[Unit]
Description=Dgraph Alpha Service
After=network.target

[Service]
User=$(whoami)
ExecStart=/usr/local/bin/dgraph alpha \\
  --my=172.30.0.36:7080 \\
  --zero=172.30.0.65:5080,172.30.0.95:5080,172.30.0.171:5080 \\
  --postings=/data/alpha/p \\
  --wal=/data/alpha/w \\
  --tmp /data/alpha/tmp \\
  --export=/data/alpha/export \\
  --cache="size-mb=18000" \\
  --security="whitelist=0.0.0.0/0" \\
  --raft="pending-proposals=4096" \\
  --limit="max-retries=10; query-edge=2000000" \\
  --trace="ratio=0" \\
  --telemetry="reports=false; sentry=false" \\
  --bindall=true \\
  --v=2
Restart=always
LimitNOFILE=33554432
LimitNPROC=33554432
LimitCORE=33554432
Environment="GOGC=50"
Environment="GOMEMLIMIT=28GiB"

[Install]
WantedBy=multi-user.target
EOF

echo "===> Enabling and starting Dgraph Alpha..."
sudo systemctl daemon-reload
sudo systemctl enable dgraph-alpha
sudo systemctl restart dgraph-alpha
sudo systemctl status dgraph-alpha --no-pager

echo "===> Setup complete!"
echo "Check Alpha health: curl http://${PRIVATE_IP}:8080/health"


rsync -avz -e "ssh -i /home/ubuntu/zero-staging.pem" \
  --progress --inplace --compress-choice=zstd \
  /data/alpha/p/ \
  ubuntu@172.30.0.36:/data/alpha/p/
