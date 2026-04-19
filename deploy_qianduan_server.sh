#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"

SSH_KEY="${SSH_KEY:-$HOME/.ssh/termius_server_ed25519}"
DEFAULT_DEPLOY_HOSTS="ubuntu@82.156.80.161 ubuntu@62.234.188.36 root@101.43.25.136"
DEPLOY_HOSTS_TEXT="${DEPLOY_HOSTS:-${DEPLOY_HOST:-$DEFAULT_DEPLOY_HOSTS}}"
REMOTE_PROJECT_DIR="${REMOTE_PROJECT_DIR:-/opt/Main_ChaoXingReserveSeat}"
REMOTE_STATIC_DIR="${REMOTE_STATIC_DIR:-/usr/share/nginx/seat_qianduan}"
SEAT_SERVICE="${SEAT_SERVICE:-seat-qianduan.service}"
DEPLOY_DRY_RUN="${DEPLOY_DRY_RUN:-0}"

LOCAL_QIANDUAN_DIR="$ROOT_DIR/qianduan"

if [[ ! -d "$LOCAL_QIANDUAN_DIR" ]]; then
  echo "Missing local qianduan directory: $LOCAL_QIANDUAN_DIR" >&2
  exit 1
fi

if [[ ! -f "$SSH_KEY" ]]; then
  echo "Missing SSH key: $SSH_KEY" >&2
  exit 1
fi

RSYNC_FLAGS=(-avz --delete)
if [[ "$DEPLOY_DRY_RUN" == "1" ]]; then
  RSYNC_FLAGS+=(--dry-run)
fi

SSH_RSH="ssh -o IdentitiesOnly=yes -i $SSH_KEY"
read -r -a DEPLOY_HOST_LIST <<< "$DEPLOY_HOSTS_TEXT"

deploy_one_host() {
  local deploy_host="$1"

  echo "[deploy_qianduan] sync local qianduan -> $deploy_host:$REMOTE_PROJECT_DIR/qianduan/"
  if ! rsync "${RSYNC_FLAGS[@]}" -e "$SSH_RSH" \
    "$LOCAL_QIANDUAN_DIR/" \
    "$deploy_host:$REMOTE_PROJECT_DIR/qianduan/"; then
    echo "[deploy_qianduan] rsync failed on $deploy_host" >&2
    return 1
  fi

  if [[ "$DEPLOY_DRY_RUN" == "1" ]]; then
    echo "[deploy_qianduan] dry-run enabled for $deploy_host, skip remote restart."
    return 0
  fi

  echo "[deploy_qianduan] compile backend, sync static files, restart services on $deploy_host"
  if ! ssh -o IdentitiesOnly=yes -i "$SSH_KEY" "$deploy_host" "
set -euo pipefail
cd '$REMOTE_PROJECT_DIR'
if [[ \"\$(id -u)\" -eq 0 ]]; then
  SUDO=\"\"
elif command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
  SUDO=\"sudo\"
else
  echo \"Remote user must be root or have passwordless sudo to restart services.\" >&2
  exit 1
fi
run_root() {
  if [[ -n \"\$SUDO\" ]]; then
    sudo \"\$@\"
  else
    \"\$@\"
  fi
}
python3 -m py_compile qianduan/server_api_example.py
run_root rsync -av --delete \
  --include 'index.html' \
  --include 'Renewal.html' \
  --include 'app.js' \
  --include 'admin.html' \
  --include 'admin.js' \
  --include 'seat.html' \
  --include 'seat.js' \
  --include 'renewal.js' \
  --include 'time-config-guide.docx' \
  --include 'styles.css' \
  --exclude '*' \
  '$REMOTE_PROJECT_DIR/qianduan/' '$REMOTE_STATIC_DIR/'
run_root systemctl restart '$SEAT_SERVICE'
run_root nginx -t
run_root systemctl reload nginx
"; then
    echo "[deploy_qianduan] remote restart failed on $deploy_host" >&2
    return 1
  fi

  echo "[deploy_qianduan] done on $deploy_host"
}

failed_hosts=()
for deploy_host in "${DEPLOY_HOST_LIST[@]}"; do
  if ! deploy_one_host "$deploy_host"; then
    failed_hosts+=("$deploy_host")
  fi
done

if (( ${#failed_hosts[@]} > 0 )); then
  echo "[deploy_qianduan] failed hosts: ${failed_hosts[*]}" >&2
  exit 1
fi

echo "[deploy_qianduan] done on all hosts: ${DEPLOY_HOST_LIST[*]}"
