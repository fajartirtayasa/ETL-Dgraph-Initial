#!/bin/bash
set -e

echo "===> Creating 4GB swap file..."

# Disable existing swap (safe even if none exists)
sudo swapoff -a || true

# Create swap file
sudo fallocate -l 4G /swapfile

# Secure permissions
sudo chmod 600 /swapfile

# Format as swap
sudo mkswap /swapfile

# Enable swap
sudo swapon /swapfile

# Persist on reboot
if ! grep -q "/swapfile" /etc/fstab; then
  echo "/swapfile swap swap defaults 0 0" | sudo tee -a /etc/fstab
fi

echo "===> Swap creation complete!"
echo "Swap status:"
swapon --show
free -h
