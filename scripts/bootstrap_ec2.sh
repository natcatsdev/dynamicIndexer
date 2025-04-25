#!/usr/bin/env bash
# ---------------------------------------------------------
# ONE-TIME SETUP for a fresh Amazon Linux 2023 box
# ---------------------------------------------------------
set -euo pipefail

# 0) adjust these two lines if you change paths/hostnames
BARE=/home/ec2-user/repos/dynamicIndexer.git
WORK=/home/ec2-user/dynamicIndexer

echo "▶ Installing base packages…"
sudo dnf install -y python39 python39-devel git nginx certbot
sudo pip3 install virtualenv

echo "▶ Creating bare repo and cloning working tree…"
sudo -u ec2-user git clone --bare --origin prod \
  git@github.com:natcatsdev/dynamicIndexer.git "$BARE"

sudo -u ec2-user git --git-dir="$BARE" --work-tree="$WORK" checkout -f main

echo "▶ Building virtualenv…"
sudo -u ec2-user python3 -m venv "$WORK/.venv"
sudo -u ec2-user "$WORK/.venv/bin/pip" install -r "$WORK/requirements.txt"

echo "▶ Copying systemd units & enabling services/timers…"
sudo cp "$WORK/systemd/"*.service "$WORK/systemd/"*.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now dynamicIndexer-api.service runboth.timer

echo "▶ Configuring Nginx…"
sudo cp "$WORK/deploy/nginx.conf" /etc/nginx/conf.d/dynamicIndexer.conf
sudo systemctl enable --now nginx

echo "▶ Obtaining TLS cert (Let’s Encrypt)…"
# Runs only if no cert exists yet
if [ ! -d /etc/letsencrypt/live/api.natcats.xyz ]; then
  sudo certbot --nginx -d api.natcats.xyz --non-interactive --agree-tos -m admin@natcats.xyz
fi

echo "✅  Bootstrap complete."
