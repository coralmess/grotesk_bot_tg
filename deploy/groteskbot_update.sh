#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR=${APP_DIR:-/home/ubuntu/LystTgFirefox}
LOG_FILE=${LOG_FILE:-/home/ubuntu/LystTgFirefox/runtime_data/logs/monitor_update.log}
PY=${PY:-/home/ubuntu/LystTgFirefox/.venv/bin/python}
LOCK_FILE=${LOCK_FILE:-/tmp/groteskbot-update.lock}

mkdir -p "$(dirname "$LOG_FILE")"

log() {
  printf "[%s] %s\n" "$(date '+%Y-%m-%d %H:%M:%S')" "$*" | tee -a "$LOG_FILE"
}

notify() {
  local message="$1"
  if [ -x "$PY" ] && [ -f /usr/local/bin/groteskbot_notify.py ]; then
    "$PY" /usr/local/bin/groteskbot_notify.py "$message" || true
  fi
}

on_error() {
  local status=$?
  local line=${BASH_LINENO[0]:-unknown}
  local command=${BASH_COMMAND:-unknown}
  log "Update failed at line ${line} with exit ${status}: ${command}"
  notify "Grotesk update failed at line ${line}: ${command}"
  exit "$status"
}

trap on_error ERR

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  log "Update already running; skipping this timer tick"
  exit 0
fi

command -v git >/dev/null 2>&1 || { log "git not installed"; exit 0; }
[ -x "$PY" ] || { log "Python venv missing: $PY"; exit 1; }
[ -d "$APP_DIR/.git" ] || { log "not a git repo: $APP_DIR"; exit 0; }

cd "$APP_DIR"
git remote get-url origin >/dev/null 2>&1 || { log "origin remote missing"; exit 0; }

branch=$(git symbolic-ref --short HEAD 2>/dev/null || echo master)
if ! git ls-remote --exit-code --heads origin "$branch" >/dev/null 2>&1; then
  if git ls-remote --exit-code --heads origin master >/dev/null 2>&1; then
    branch=master
  elif git ls-remote --exit-code --heads origin main >/dev/null 2>&1; then
    branch=main
  else
    log "no remote branch found"
    exit 0
  fi
fi

git fetch origin "$branch" --prune
local=$(git rev-parse HEAD)
remote=$(git rev-parse "origin/$branch")

if [ "$local" = "$remote" ]; then
  log "No updates"
  exit 0
fi

log "Updates found: ${local:0:7} -> ${remote:0:7}"

if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  log "Tracked local changes are present; aborting update before reset"
  git status --short --untracked-files=no | tee -a "$LOG_FILE"
  notify "Grotesk update blocked: tracked local changes on instance"
  exit 1
fi

changed_files=$(git diff --name-only "${local}..${remote}")
log "Changed files:"
printf "%s\n" "$changed_files" | sed 's/^/ - /' | tee -a "$LOG_FILE"

git reset --hard "origin/$branch"

if printf "%s\n" "$changed_files" | grep -qx "requirements.txt"; then
  log "requirements.txt changed; installing dependencies"
  "$PY" -m pip install -r "$APP_DIR/requirements.txt" >> "$LOG_FILE" 2>&1
else
  log "requirements.txt unchanged; skipping dependency install"
fi

if [ ! -f "$APP_DIR/deploy/restart_changed_services.py" ]; then
  log "Selective restart planner missing; refusing to guess service restarts"
  notify "Grotesk update failed: restart planner missing"
  exit 1
fi

"$PY" "$APP_DIR/deploy/restart_changed_services.py" --from-ref "$local" --to-ref "$remote" >> "$LOG_FILE" 2>&1
notify "Grotesk services updated: ${local:0:7} -> ${remote:0:7}"
log "Update completed: ${local:0:7} -> ${remote:0:7}"
