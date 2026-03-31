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

if ! command -v lftp >/dev/null 2>&1; then
  echo "lftp is not installed" >&2
  exit 1
fi

lftp -u "$DEPLOY_USER","$DEPLOY_PASSWORD" "sftp://$DEPLOY_HOST:$DEPLOY_PORT" <<EOF
set sftp:auto-confirm yes
set net:timeout 30
set net:max-retries 2
set net:persist-retries 1
mkdir -p "$REMOTE_ROOT"
mirror --reverse --verbose --delete \
  --exclude-glob .git* \
  --exclude-glob .github* \
  --exclude-glob .cpanel.yml \
  --exclude-glob .htpasswd \
  --exclude-glob .DS_Store \
  --exclude-glob README.md \
  --exclude-glob scripts/* \
  . "$REMOTE_ROOT"
mkdir -p "$REMOTE_PORTAL_AUTH_DIR"
put "$tmpdir/.htpasswd" -o "$REMOTE_PORTAL_AUTH_DIR/.htpasswd"
bye
EOF
