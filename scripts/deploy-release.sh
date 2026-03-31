#!/usr/bin/env bash
set -euo pipefail

DEPLOY_HOST="${DEPLOY_HOST:?Set DEPLOY_HOST}"
DEPLOY_USER="${DEPLOY_USER:?Set DEPLOY_USER}"
DEPLOY_PASSWORD="${DEPLOY_PASSWORD:?Set DEPLOY_PASSWORD}"
DEPLOY_PORT="${DEPLOY_PORT:-21098}"
REMOTE_ROOT="${REMOTE_ROOT:-/home/insubhmy/public_html}"
REMOTE_PORTAL_AUTH_DIR="${REMOTE_PORTAL_AUTH_DIR:-/home/insubhmy/portal-auth}"
AUTH_FILE_LOCAL="${AUTH_FILE_LOCAL:-.htpasswd}"

if [[ ! -f "$AUTH_FILE_LOCAL" ]]; then
  echo "Missing auth file: $AUTH_FILE_LOCAL" >&2
  exit 1
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT
cp "$AUTH_FILE_LOCAL" "$tmpdir/.htpasswd"

if ! command -v rsync >/dev/null 2>&1; then
  echo "rsync is not installed" >&2
  exit 1
fi

if ! command -v sshpass >/dev/null 2>&1; then
  echo "sshpass is not installed" >&2
  exit 1
fi

export SSHPASS="$DEPLOY_PASSWORD"
SSH_CMD="ssh -p ${DEPLOY_PORT} -o StrictHostKeyChecking=accept-new -o ServerAliveInterval=15 -o ServerAliveCountMax=2 -o ConnectTimeout=20"

sshpass -e ssh -p "$DEPLOY_PORT" -o StrictHostKeyChecking=accept-new -o ServerAliveInterval=15 -o ServerAliveCountMax=2 -o ConnectTimeout=20 \
  "$DEPLOY_USER@$DEPLOY_HOST" "mkdir -p '$REMOTE_ROOT' '$REMOTE_PORTAL_AUTH_DIR'"

sshpass -e rsync -az --delete --delete-excluded \
  --exclude '.git/' \
  --exclude '.github/' \
  --exclude '.cpanel.yml' \
  --exclude '.htpasswd' \
  --exclude '.DS_Store' \
  --exclude 'README.md' \
  --exclude 'scripts/' \
  --exclude 'google-intake/' \
  --exclude 'portal/README.md' \
  --exclude 'portal/.gitignore' \
  -e "$SSH_CMD" \
  ./ "$DEPLOY_USER@$DEPLOY_HOST:$REMOTE_ROOT/"

sshpass -e rsync -az \
  -e "$SSH_CMD" \
  "$tmpdir/.htpasswd" "$DEPLOY_USER@$DEPLOY_HOST:$REMOTE_PORTAL_AUTH_DIR/.htpasswd"
