"""Smoke-test the BUILT image's web listeners with disposable resources.

    docker build -t apreferral-listener-smoke .
    python scripts/smoke_web_listeners_image.py --image apreferral-listener-smoke

Runs the image's own gunicorn with the real ``gunicorn_web.conf.py`` against
``scripts/listener_smoke_app.py`` (the real V2 report blueprint and private-port
guard over a stub database, because ``main:app`` needs a live MongoDB), on the
host network, and checks:

* public IPv4 listener: ``/healthz`` 200, the app is served, the report is 404;
* private IPv6 listener ``[::1]:<port>``: report 401 without/with a wrong
  bearer, 403 with a Fly edge header, 200 with the token, everything else 404.

No Fly access, no credentials, no data. Exits non-zero (with the container log)
on the first failed expectation. Needs a Linux Docker host with IPv6 loopback.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

PUBLIC, PRIVATE = 18080, 18081
TOKEN = "image-smoke-token"
REPORT = "/internal/affiliate-v2-report/v1/summary?start_date=2026-10-01&end_date=2026-10-31"


def get(url, headers=None):
    request = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(request, timeout=5) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    args = parser.parse_args()
    name = "apreferral-listener-smoke-" + uuid.uuid4().hex[:8]
    run = subprocess.run([
        "docker", "run", "-d", "--name", name, "--network", "host",
        "-e", f"WEB_PUBLIC_BIND=127.0.0.1:{PUBLIC}", "-e", "AFFILIATE_V2_REPORT_ENABLED=true",
        "-e", f"AFFILIATE_V2_REPORT_PRIVATE_PORT={PRIVATE}", "-e", f"AFFILIATE_V2_REPORT_TOKEN={TOKEN}",
        args.image, "gunicorn", "scripts.listener_smoke_app:app", "-c", "gunicorn_web.conf.py",
        "--workers", "1", "--threads", "2", "--error-logfile", "-",
    ], capture_output=True, text=True)
    if run.returncode:
        print(run.stderr, file=sys.stderr)
        return 1
    failures = []

    def expect(label, got, want):
        ok = got == want
        print(("ok   " if ok else "FAIL ") + f"{label}: got {got!r}, want {want!r}")
        if not ok:
            failures.append(label)

    try:
        deadline = time.time() + 60
        while True:
            try:
                if get(f"http://127.0.0.1:{PUBLIC}/healthz")[0] == 200:
                    break
            except OSError:
                pass
            if time.time() > deadline:
                failures.append("public listener never became ready")
                break
            time.sleep(0.5)
        if not failures:
            pub, priv = f"http://127.0.0.1:{PUBLIC}", f"http://[::1]:{PRIVATE}"
            auth = {"Authorization": f"Bearer {TOKEN}"}
            expect("public /healthz", get(f"{pub}/healthz"), (200, "ok\n"))
            expect("public app", get(f"{pub}/miniapp")[0], 200)
            expect("public report (token) is not served", get(f"{pub}{REPORT}", auth)[0], 404)
            expect("private no token", get(f"{priv}{REPORT}")[0], 401)
            expect("private wrong token", get(f"{priv}{REPORT}", {"Authorization": "Bearer x"})[0], 401)
            expect("private via Fly edge header", get(f"{priv}{REPORT}", {**auth, "Fly-Client-IP": "9.9.9.9"})[0], 403)
            status, body = get(f"{priv}{REPORT}", auth)
            expect("private with token", status, 200)
            expect("private body is a preview with no effects",
                   '"report_mode":"preview"' in body.replace(" ", "") and '"effects_written":false' in body.replace(" ", ""), True)
            expect("private /healthz", get(f"{priv}/healthz", auth)[0], 404)
            expect("private /miniapp", get(f"{priv}/miniapp", auth)[0], 404)
    finally:
        log = subprocess.run(["docker", "logs", name], capture_output=True, text=True)
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)
        if failures:
            print("--- container log ---\n" + log.stdout + log.stderr)
    print("PASS" if not failures else f"FAILED: {failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
