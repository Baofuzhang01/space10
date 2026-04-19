#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"

SSH_KEY="${SSH_KEY:-$HOME/.ssh/termius_server_ed25519}"
DEFAULT_DEPLOY_HOSTS="ubuntu@82.156.80.161 ubuntu@62.234.188.36 root@101.43.25.136"
DEPLOY_HOSTS_TEXT="${DEPLOY_HOSTS:-${DEPLOY_HOST:-$DEFAULT_DEPLOY_HOSTS}}"
REMOTE_PROJECT_DIR="${REMOTE_PROJECT_DIR:-/opt/Main_ChaoXingReserveSeat}"
REMOTE_STATIC_DIR="${REMOTE_STATIC_DIR:-/usr/share/nginx/seat_qianduan}"
SEAT_SERVICE="${SEAT_SERVICE:-seat-qianduan.service}"
DISPATCH_SERVICE="${DISPATCH_SERVICE:-server-dispatch.service}"
DEPLOY_DRY_RUN="${DEPLOY_DRY_RUN:-0}"

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

  echo "[deploy_server] sync project -> $deploy_host:$REMOTE_PROJECT_DIR/"
  if ! rsync "${RSYNC_FLAGS[@]}" \
    --exclude '.git' \
    --exclude '.idea' \
    --exclude '.venv' \
    --exclude '.DS_Store' \
    --exclude '.sync_shared_files_cache' \
    --exclude '__MACOSX' \
    --exclude '__pycache__' \
    --exclude '*.pyc' \
    --exclude 'config.json' \
    --exclude 'utils/config.json' \
    --exclude 'seat-qianduan.env.local' \
    --exclude 'html_debug' \
    --exclude 'logs' \
    --exclude 'pageexe' \
    --exclude 'pageexe.zip' \
    --exclude 'server_runs' \
    --exclude 'server_worker2_watchdog' \
    --exclude 'server_store/*.sqlite3' \
    --exclude 'server_store/*.sqlite3-*' \
    --exclude 'worker2/.wrangler' \
    --exclude 'workers/tongyi/.wrangler' \
    --exclude 'workers/tongyi/dist' \
    --exclude 'workers/tongyi/node_modules' \
    --exclude 'worker2/node_modules' \
    -e "$SSH_RSH" \
    "$ROOT_DIR/" \
    "$deploy_host:$REMOTE_PROJECT_DIR/"; then
    echo "[deploy_server] rsync failed on $deploy_host" >&2
    return 1
  fi

  if [[ "$DEPLOY_DRY_RUN" == "1" ]]; then
    echo "[deploy_server] dry-run enabled for $deploy_host, skip remote restart."
    return 0
  fi

  echo "[deploy_server] compile backend, sync static files, restart services on $deploy_host"
  if ! ssh -o IdentitiesOnly=yes -i "$SSH_KEY" "$deploy_host" "
set -euo pipefail
cd '$REMOTE_PROJECT_DIR'
if [[ \"\$(id -u)\" -eq 0 ]]; then
  SUDO=\"\"
elif command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
  SUDO=\"sudo\"
else
  echo \"Remote user must be root or have passwordless sudo to update services.\" >&2
  exit 1
fi
run_root() {
  if [[ -n \"\$SUDO\" ]]; then
    sudo \"\$@\"
  else
    \"\$@\"
  fi
}
if command -v apt-get >/dev/null 2>&1; then
  run_root env DEBIAN_FRONTEND=noninteractive apt-get update
  run_root env DEBIAN_FRONTEND=noninteractive apt-get install -y libgl1 libglib2.0-0
fi
if [[ -x '$REMOTE_PROJECT_DIR/.venv/bin/python' ]]; then
  run_root '$REMOTE_PROJECT_DIR/.venv/bin/pip' uninstall -y opencv-python opencv-contrib-python opencv-contrib-python-headless cv2 >/dev/null 2>&1 || true
  run_root '$REMOTE_PROJECT_DIR/.venv/bin/pip' install -r requirements.txt
  run_root '$REMOTE_PROJECT_DIR/.venv/bin/python' -m py_compile main.py server_dispatch.py qianduan/server_api_example.py server_store/repository.py
  run_root '$REMOTE_PROJECT_DIR/.venv/bin/python' - <<'PY'
import cv2
if not hasattr(cv2, 'imdecode'):
    raise SystemExit('cv2.imdecode missing after dependency install')
print('cv2 ok', getattr(cv2, '__version__', 'unknown'))
PY
else
  python3 -m py_compile main.py server_dispatch.py qianduan/server_api_example.py server_store/repository.py
fi
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
run_root systemctl restart '$DISPATCH_SERVICE' || true
run_root nginx -t
run_root systemctl reload nginx
"; then
    echo "[deploy_server] remote restart failed on $deploy_host" >&2
    return 1
  fi

  echo "[deploy_server] done on $deploy_host"
}

failed_hosts=()
for deploy_host in "${DEPLOY_HOST_LIST[@]}"; do
  if ! deploy_one_host "$deploy_host"; then
    failed_hosts+=("$deploy_host")
  fi
done

if (( ${#failed_hosts[@]} > 0 )); then
  echo "[deploy_server] failed hosts: ${failed_hosts[*]}" >&2
  exit 1
fi

echo "[deploy_server] done on all hosts: ${DEPLOY_HOST_LIST[*]}"
