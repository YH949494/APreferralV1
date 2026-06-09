#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${JUPYTER_TOKEN:-}" ]]; then
  echo "ERROR: JUPYTER_TOKEN is required. Set it with: fly secrets set JUPYTER_TOKEN=..." >&2
  exit 1
fi

if [[ "${JUPYTER_TOKEN}" == "none" || "${JUPYTER_TOKEN}" == "None" ]]; then
  echo "ERROR: Refusing to start with an unsafe JUPYTER_TOKEN value." >&2
  exit 1
fi

if (( ${#JUPYTER_TOKEN} < 32 )); then
  echo "ERROR: JUPYTER_TOKEN must be at least 32 characters." >&2
  exit 1
fi

WORK_DIR="${JUPYTER_WORK_DIR:-/tmp/jupyter-work}"
mkdir -p "${WORK_DIR}"
chmod 0700 "${WORK_DIR}"

# Keep uploaded/generated analysis files ephemeral by default. Fly Machine root
# filesystems can survive restarts, so clear the runtime workspace on boot.
find "${WORK_DIR}" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +

exec start-notebook.py \
  --ServerApp.ip=0.0.0.0 \
  --ServerApp.port=8888 \
  --ServerApp.open_browser=False \
  --ServerApp.token="${JUPYTER_TOKEN}" \
  --ServerApp.allow_origin='*' \
  --ServerApp.root_dir="${WORK_DIR}"
