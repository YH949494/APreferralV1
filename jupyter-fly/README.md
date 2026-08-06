# Fly.io Jupyter service for OpenWebUI Code Interpreter

This directory defines a separate Fly.io Jupyter service for OpenWebUI Code Interpreter.
It is intentionally independent from the existing OpenWebUI app and does not define a Fly volume.

## Folder structure

```text
jupyter-fly/
  Dockerfile
  start-jupyter.sh
  fly.toml
  .dockerignore
  README.md
```

## Files

- `Dockerfile` builds from `quay.io/jupyter/scipy-notebook:2025-12-31` and explicitly installs `pandas`, `numpy`, `openpyxl`, `xlsxwriter`, `matplotlib`, and `scipy`.
- `start-jupyter.sh` fails closed when `JUPYTER_TOKEN` is missing, unsafe, or shorter than 32 characters, clears `/tmp/jupyter-work` at boot, and starts Jupyter on `0.0.0.0:8888`.
- `fly.toml` exposes only port `8888` through Fly HTTPS, keeps the machine running, and requests `performance-2x` with `4gb` RAM.

## Fly CLI deployment

Choose a unique app name and the region closest to your OpenWebUI app/users. The example region is `iad`; change `primary_region` in `fly.toml` if needed before deploy.

```bash
cd /workspace/APreferralV1/jupyter-fly
export JUPYTER_APP="openwebui-jupyter-ci-$(openssl rand -hex 3)"
export FLY_ORG="personal"

fly apps create "$JUPYTER_APP" --org "$FLY_ORG"
fly secrets set --app "$JUPYTER_APP" JUPYTER_TOKEN="$(openssl rand -hex 32)"
fly deploy --config fly.toml --app "$JUPYTER_APP" --remote-only
fly scale vm performance-2x --memory 4096 --app "$JUPYTER_APP"
fly scale count 1 --app "$JUPYTER_APP"
fly status --app "$JUPYTER_APP"
```

If your Fly plan or region does not allow `performance-2x`, use the closest valid fallback:

```bash
fly scale vm shared-cpu-2x --memory 4096 --app "$JUPYTER_APP"
```

## OpenWebUI Admin Settings

Configure only OpenWebUI Admin Settings after the Jupyter app is deployed:

- Code Interpreter Engine: `Jupyter`
- Jupyter URL: `https://<JUPYTER_APP>.fly.dev`
- Jupyter Auth: `Token`
- Jupyter Token: the same value stored in Fly secret `JUPYTER_TOKEN`
- Timeout: `300` seconds

Do not hardcode the token in this repository, Dockerfile, or `fly.toml`.

## Smoke test

Create a local multi-sheet workbook:

```bash
python - <<'PY'
from pathlib import Path
import pandas as pd

path = Path('/tmp/openwebui-code-interpreter-smoke.xlsx')
with pd.ExcelWriter(path, engine='xlsxwriter') as writer:
    pd.DataFrame({'region': ['East', 'West', 'East'], 'revenue': [100, 200, 150]}).to_excel(writer, sheet_name='Sales', index=False)
    pd.DataFrame({'region': ['East', 'West'], 'target': [220, 180]}).to_excel(writer, sheet_name='Targets', index=False)
print(path)
PY
```

Upload it to OpenWebUI and ask Code Interpreter:

```text
Read all sheets from this Excel file with pandas. Show revenue by region, compare to targets, and create a bar chart.
```

Expected result: pandas reads both sheets, returns a KPI summary, and produces a chart without import errors for `pandas`, `openpyxl`, `xlsxwriter`, `matplotlib`, `numpy`, or `scipy`.

## Rollback / disable

Disable Code Interpreter in OpenWebUI first, or clear the Jupyter URL/token fields. Then stop or remove the separate Jupyter Fly app:

```bash
fly scale count 0 --app "$JUPYTER_APP"
# Permanent removal after confirming OpenWebUI no longer depends on it:
fly apps destroy "$JUPYTER_APP"
```

Rotate the token without rebuilding the image:

```bash
fly secrets set --app "$JUPYTER_APP" JUPYTER_TOKEN="$(openssl rand -hex 32)"
```
