#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Fill these before first use, or provide them via environment variables.
SSH_KEY="${SSH_KEY:-}"
DEPLOY_HOST="${DEPLOY_HOST:-}"
SERVER_NAME="${SERVER_NAME:-}"
LOCAL_ENV_FILE="${LOCAL_ENV_FILE:-}"

REMOTE_PROJECT_DIR="${REMOTE_PROJECT_DIR:-/opt/Main_ChaoXingReserveSeat}"
REMOTE_STATIC_DIR="${REMOTE_STATIC_DIR:-/usr/share/nginx/seat_qianduan}"
REMOTE_ENV_FILE="${REMOTE_ENV_FILE:-/etc/seat-qianduan.env}"
REMOTE_STAGE_DIR="${REMOTE_STAGE_DIR:-/tmp/Main_ChaoXingReserveSeat-bootstrap}"
VENV_DIR="${VENV_DIR:-$REMOTE_PROJECT_DIR/.venv}"
SEAT_SERVICE="${SEAT_SERVICE:-seat-qianduan.service}"
DISPATCH_SERVICE="${DISPATCH_SERVICE:-server-dispatch.service}"
QIANDUAN_PORT="${QIANDUAN_PORT:-8090}"
DISPATCH_PORT="${DISPATCH_PORT:-8788}"
RUN_INIT_LOCAL_CACHE="${RUN_INIT_LOCAL_CACHE:-1}"
ENABLE_SYNC_TIMERS="${ENABLE_SYNC_TIMERS:-1}"
ENABLE_RENEWAL_TIMER="${ENABLE_RENEWAL_TIMER:-1}"
REMOTE_STAGE_PROJECT_DIR="$REMOTE_STAGE_DIR/project"
REMOTE_STAGE_ENV_FILE="$REMOTE_STAGE_DIR/seat-qianduan.env"

required_vars=(
  SSH_KEY
  DEPLOY_HOST
  SERVER_NAME
  LOCAL_ENV_FILE
)

for name in "${required_vars[@]}"; do
  if [[ -z "${!name}" ]]; then
    echo "Missing required setting: $name" >&2
    echo "Fill it at the top of bootstrap_new_server.sh or export it before running." >&2
    exit 1
  fi
done

if [[ ! -f "$SSH_KEY" ]]; then
  echo "Missing SSH key: $SSH_KEY" >&2
  exit 1
fi

if [[ ! -f "$LOCAL_ENV_FILE" ]]; then
  echo "Missing local env file: $LOCAL_ENV_FILE" >&2
  exit 1
fi

SSH_RSH="ssh -o IdentitiesOnly=yes -i $SSH_KEY"

echo "[bootstrap] verify remote sudo and install base packages on $DEPLOY_HOST"
ssh -o IdentitiesOnly=yes -i "$SSH_KEY" "$DEPLOY_HOST" 'bash -s' -- "$REMOTE_STAGE_DIR" <<'REMOTE_BOOTSTRAP'
set -euo pipefail

STAGE_DIR="$1"

if [[ "$(id -u)" -eq 0 ]]; then
  SUDO=""
elif command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
  SUDO="sudo"
else
  echo "Remote user must be root or have passwordless sudo." >&2
  exit 1
fi

run_root() {
  if [[ -n "$SUDO" ]]; then
    sudo "$@"
  else
    "$@"
  fi
}

mkdir -p "$STAGE_DIR"
chmod 700 "$STAGE_DIR"

if command -v apt-get >/dev/null 2>&1; then
  run_root env DEBIAN_FRONTEND=noninteractive apt-get update
  run_root env DEBIAN_FRONTEND=noninteractive apt-get install -y python3 python3-venv python3-pip nginx rsync curl libgl1 libglib2.0-0
elif command -v dnf >/dev/null 2>&1; then
  run_root dnf install -y python3 python3-pip python3-virtualenv nginx rsync curl mesa-libGL glib2
elif command -v yum >/dev/null 2>&1; then
  run_root yum install -y python3 python3-pip python3-virtualenv nginx rsync curl mesa-libGL glib2
else
  echo "Unsupported package manager. Install python3, python3-pip, nginx, rsync, curl, libgl and glib manually." >&2
  exit 1
fi
REMOTE_BOOTSTRAP

echo "[bootstrap] sync project -> $DEPLOY_HOST:$REMOTE_STAGE_PROJECT_DIR/"
rsync -avz --delete \
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
  "$DEPLOY_HOST:$REMOTE_STAGE_PROJECT_DIR/"

echo "[bootstrap] upload env file -> $REMOTE_STAGE_ENV_FILE"
rsync -avz -e "$SSH_RSH" \
  "$LOCAL_ENV_FILE" \
  "$DEPLOY_HOST:$REMOTE_STAGE_ENV_FILE"

echo "[bootstrap] configure services, nginx, virtualenv and local cache"
ssh -o IdentitiesOnly=yes -i "$SSH_KEY" "$DEPLOY_HOST" \
  bash -s -- \
  "$REMOTE_STAGE_DIR" \
  "$REMOTE_STAGE_PROJECT_DIR" \
  "$REMOTE_STAGE_ENV_FILE" \
  "$REMOTE_PROJECT_DIR" \
  "$REMOTE_STATIC_DIR" \
  "$REMOTE_ENV_FILE" \
  "$VENV_DIR" \
  "$SEAT_SERVICE" \
  "$DISPATCH_SERVICE" \
  "$SERVER_NAME" \
  "$QIANDUAN_PORT" \
  "$DISPATCH_PORT" \
  "$RUN_INIT_LOCAL_CACHE" \
  "$ENABLE_SYNC_TIMERS" \
  "$ENABLE_RENEWAL_TIMER" <<'REMOTE_CONFIG'
set -euo pipefail

STAGE_DIR="$1"
STAGE_PROJECT_DIR="$2"
STAGE_ENV_FILE="$3"
PROJECT_DIR="$4"
STATIC_DIR="$5"
ENV_FILE="$6"
VENV_DIR="$7"
SEAT_SERVICE="$8"
DISPATCH_SERVICE="$9"
SERVER_NAME="${10}"
QIANDUAN_PORT="${11}"
DISPATCH_PORT="${12}"
RUN_INIT_LOCAL_CACHE="${13}"
ENABLE_SYNC_TIMERS="${14}"
ENABLE_RENEWAL_TIMER="${15}"

if [[ "$(id -u)" -eq 0 ]]; then
  SUDO=""
elif command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
  SUDO="sudo"
else
  echo "Remote user must be root or have passwordless sudo." >&2
  exit 1
fi

run_root() {
  if [[ -n "$SUDO" ]]; then
    sudo "$@"
  else
    "$@"
  fi
}

run_root install -d -m 755 "$PROJECT_DIR" "$STATIC_DIR" "$PROJECT_DIR/server_runs"
run_root rsync -av --delete \
  --exclude '.venv' \
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
  "$STAGE_PROJECT_DIR/" "$PROJECT_DIR/"
run_root install -m 600 "$STAGE_ENV_FILE" "$ENV_FILE"

if [[ ! -x "$VENV_DIR/bin/python" ]]; then
  run_root python3 -m venv "$VENV_DIR"
fi

run_root "$VENV_DIR/bin/pip" install --upgrade pip setuptools wheel
run_root "$VENV_DIR/bin/pip" uninstall -y opencv-python opencv-contrib-python opencv-contrib-python-headless cv2 >/dev/null 2>&1 || true
run_root "$VENV_DIR/bin/pip" install -r "$PROJECT_DIR/requirements.txt"

run_root "$VENV_DIR/bin/python" -m py_compile \
  "$PROJECT_DIR/main.py" \
  "$PROJECT_DIR/server_dispatch.py" \
  "$PROJECT_DIR/qianduan/server_api_example.py" \
  "$PROJECT_DIR/server_store/repository.py" \
  "$PROJECT_DIR/server_store/renewal_scan.py"
run_root "$VENV_DIR/bin/python" - <<'PY'
import cv2
if not hasattr(cv2, "imdecode"):
    raise SystemExit("cv2.imdecode missing after dependency install")
print("cv2 ok", getattr(cv2, "__version__", "unknown"))
PY

run_root tee "/etc/systemd/system/$SEAT_SERVICE" >/dev/null <<EOF
[Unit]
Description=Seat Qianduan Gunicorn Service
After=network.target

[Service]
Type=simple
WorkingDirectory=$PROJECT_DIR
EnvironmentFile=$ENV_FILE
Environment=PYTHONUNBUFFERED=1
ExecStart=$VENV_DIR/bin/gunicorn --workers 2 --threads 4 --bind 127.0.0.1:$QIANDUAN_PORT --timeout 120 qianduan.server_api_example:app
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

run_root tee "/etc/systemd/system/$DISPATCH_SERVICE" >/dev/null <<EOF
[Unit]
Description=Seat Server Dispatch Service
After=network.target

[Service]
Type=simple
WorkingDirectory=$PROJECT_DIR
EnvironmentFile=$ENV_FILE
Environment=PYTHONUNBUFFERED=1
ExecStart=$VENV_DIR/bin/python $PROJECT_DIR/server_dispatch.py
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

run_root tee /etc/systemd/system/seat-sync-push.service >/dev/null <<EOF
[Unit]
Description=Seat Store Incremental Push Sync
After=network.target

[Service]
Type=oneshot
WorkingDirectory=$PROJECT_DIR
EnvironmentFile=$ENV_FILE
ExecStart=$VENV_DIR/bin/python $PROJECT_DIR/server_store/sync_push.py
EOF

run_root tee /etc/systemd/system/seat-sync-push.timer >/dev/null <<EOF
[Unit]
Description=Run Seat Store Incremental Push Sync Every 5 Minutes

[Timer]
OnBootSec=2min
OnUnitActiveSec=5min
Unit=seat-sync-push.service

[Install]
WantedBy=timers.target
EOF

run_root tee /etc/systemd/system/seat-sync-pull.service >/dev/null <<EOF
[Unit]
Description=Seat Store Full Pull Sync
After=network.target

[Service]
Type=oneshot
WorkingDirectory=$PROJECT_DIR
EnvironmentFile=$ENV_FILE
ExecStart=$VENV_DIR/bin/python $PROJECT_DIR/server_store/sync_pull.py
EOF

run_root tee /etc/systemd/system/seat-sync-pull.timer >/dev/null <<EOF
[Unit]
Description=Run Seat Store Full Pull Sync Every Day At 03:00

[Timer]
OnBootSec=10min
OnCalendar=*-*-* 03:00:00
Persistent=true
Unit=seat-sync-pull.service

[Install]
WantedBy=timers.target
EOF

run_root tee /etc/systemd/system/seat-renewal-scan.service >/dev/null <<EOF
[Unit]
Description=Seat renewal daily scan
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
WorkingDirectory=$PROJECT_DIR
EnvironmentFile=$ENV_FILE
ExecStart=$VENV_DIR/bin/python $PROJECT_DIR/server_store/renewal_scan.py
EOF

run_root tee /etc/systemd/system/seat-renewal-scan.timer >/dev/null <<EOF
[Unit]
Description=Run Seat Renewal Daily Scan At 23:00

[Timer]
OnCalendar=*-*-* 23:00:00
Persistent=true
Unit=seat-renewal-scan.service

[Install]
WantedBy=timers.target
EOF

run_root tee /etc/nginx/conf.d/seat.conf >/dev/null <<EOF
server {
    listen 80;
    server_name $SERVER_NAME;

    root $STATIC_DIR;
    index index.html;

    location / {
        try_files \$uri \$uri/ /index.html;
    }

    location = /dispatch {
        proxy_pass http://127.0.0.1:$DISPATCH_PORT;
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }

    location /api/ {
        proxy_pass http://127.0.0.1:$QIANDUAN_PORT;
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }

    location = /health {
        proxy_pass http://127.0.0.1:$QIANDUAN_PORT/health;
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
    }
}
EOF

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
  "$PROJECT_DIR/qianduan/" "$STATIC_DIR/"

if [[ "$RUN_INIT_LOCAL_CACHE" == "1" ]]; then
  run_root "$VENV_DIR/bin/python" "$PROJECT_DIR/server_store/init_local_cache.py"
fi

run_root systemctl daemon-reload
run_root systemctl enable --now nginx
run_root systemctl enable --now "$SEAT_SERVICE"
run_root systemctl enable --now "$DISPATCH_SERVICE"

if [[ "$ENABLE_SYNC_TIMERS" == "1" ]]; then
  run_root systemctl enable --now seat-sync-push.timer
  run_root systemctl enable --now seat-sync-pull.timer
fi

if [[ "$ENABLE_RENEWAL_TIMER" == "1" ]]; then
  run_root systemctl enable --now seat-renewal-scan.timer
fi

run_root nginx -t
run_root systemctl reload nginx
rm -rf "$STAGE_DIR"
REMOTE_CONFIG

echo "[bootstrap] done."
