"""Web listeners: the public listener/health check are untouched and the V2 report
has a private IPv6 listener that is bearer-authenticated and not publicly usable.

Real gunicorn subprocesses run the real ``gunicorn_web.conf.py`` against
``scripts/listener_smoke_app.py``. IPv6 assertions need an IPv6-capable host:
locally they skip when the kernel has none; in CI (``CI=true``) a missing IPv6
loopback FAILS the test instead of skipping it.
"""
import os
import re
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest
from flask import Flask

import affiliate_v2_report_api as api
import gunicorn_web_conf_loader as loader

ROOT = Path(__file__).resolve().parent
TOKEN = "listener-smoke-token"
PREFIX = api.PREFIX


def ipv6_loopback() -> bool:
    try:
        s = socket.socket(socket.AF_INET6)
    except OSError:
        return False
    try:
        s.bind(("::1", 0))
        return True
    except OSError:
        return False
    finally:
        s.close()


def require_ipv6():
    if ipv6_loopback():
        return
    if os.environ.get("CI", "").lower() in ("1", "true", "yes"):
        pytest.fail("no IPv6 loopback on the CI runner - the private listener would go untested")
    pytest.skip("this host has no IPv6; the private-listener assertions run in CI")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# --- configuration -------------------------------------------------------------------

conf = loader.load()


def test_default_is_exactly_the_old_public_listener():
    assert conf.build_bind({}, ipv6_ok=lambda: True) == ["0.0.0.0:8080"]
    assert conf.bind == ["0.0.0.0:8080"] or os.environ.get("AFFILIATE_V2_REPORT_PRIVATE_PORT")


@pytest.mark.parametrize("env", [
    {"AFFILIATE_V2_REPORT_PRIVATE_PORT": "8081"},                                   # flag off
    {"AFFILIATE_V2_REPORT_ENABLED": "true"},                                        # no port
    {"AFFILIATE_V2_REPORT_ENABLED": "false", "AFFILIATE_V2_REPORT_PRIVATE_PORT": "8081"},
    {"AFFILIATE_V2_REPORT_ENABLED": "true", "AFFILIATE_V2_REPORT_PRIVATE_PORT": "8080"},   # conflicts with public
    {"AFFILIATE_V2_REPORT_ENABLED": "true", "AFFILIATE_V2_REPORT_PRIVATE_PORT": "80"},     # privileged
    {"AFFILIATE_V2_REPORT_ENABLED": "true", "AFFILIATE_V2_REPORT_PRIVATE_PORT": "abc"},
    {"AFFILIATE_V2_REPORT_ENABLED": "true", "AFFILIATE_V2_REPORT_PRIVATE_PORT": "70000"},
])
def test_private_listener_needs_flag_and_a_valid_distinct_port(env):
    assert conf.build_bind(env, ipv6_ok=lambda: True) == ["0.0.0.0:8080"]


def test_private_listener_is_added_after_the_public_one_only_when_configured_and_ipv6_exists():
    env = {"AFFILIATE_V2_REPORT_ENABLED": "TRUE", "AFFILIATE_V2_REPORT_PRIVATE_PORT": " 8081 "}
    assert conf.build_bind(env, ipv6_ok=lambda: True) == ["0.0.0.0:8080", "[::]:8081"]
    assert conf.build_bind(env, ipv6_ok=lambda: False) == ["0.0.0.0:8080"]      # never blocks the public listener


def test_fly_config_keeps_the_public_service_and_health_check_and_adds_no_public_port():
    text = (ROOT / "fly.toml").read_text()
    web = re.search(r'^\s*web = "(.*)"\s*$', text, re.M).group(1)
    assert web.startswith("gunicorn main:app -c gunicorn_web.conf.py ") and "--bind" not in web
    for flag in ("--workers 2", "--worker-class gthread", "--threads 4", "--timeout 30", "--graceful-timeout 30"):
        assert flag in web
    service = text.split("[[services]]", 1)[1]
    assert 'processes = ["web"]' in service and "internal_port = 8080" in service
    ports = re.findall(r"^\s*port = (\d+)", service, re.M)
    assert ports == ["80", "443"], "only the edge ports may be mapped; the private port must never be"
    assert "8081" not in service
    check = re.search(r"\[\[services\.http_checks\]\]([^\[]+)", service).group(1)
    assert 'path = "/healthz"' in check and 'method = "GET"' in check and 'protocol = "http"' in check


# --- application guard ---------------------------------------------------------------

@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("AFFILIATE_V2_REPORT_ENABLED", "true")
    monkeypatch.setenv("AFFILIATE_V2_REPORT_TOKEN", TOKEN)
    monkeypatch.setenv("AFFILIATE_V2_REPORT_PRIVATE_PORT", "8081")
    monkeypatch.delenv("AFFILIATE_V2_REPORT_ALLOW_PUBLIC", raising=False)
    monkeypatch.delenv("DATABOT_API_KEY", raising=False)


def smoke_client():
    from scripts.listener_smoke_app import app
    return app.test_client()


URL = f"{PREFIX}/summary?start_date=2026-10-01&end_date=2026-10-31"
AUTH = {"Authorization": f"Bearer {TOKEN}"}


def test_report_route_is_only_served_on_the_private_listener(env):
    c = smoke_client()
    public = {"environ_overrides": {"SERVER_PORT": "8080"}}
    assert c.get(URL, headers=AUTH, **public).status_code == 404            # even with the right token
    assert c.get(URL, headers={**AUTH, "Fly-Client-IP": "1.2.3.4"}, **public).status_code == 404
    private = {"environ_overrides": {"SERVER_PORT": "8081"}}
    assert c.get(URL, **private).status_code == 401
    assert c.get(URL, headers={"Authorization": "Bearer nope"}, **private).status_code == 401
    ok = c.get(URL, headers=AUTH, **private)
    assert ok.status_code == 200 and ok.get_json()["report_mode"] == "preview" and ok.get_json()["effects_written"] is False
    assert c.get(URL, headers={**AUTH, "Fly-Client-IP": "1.2.3.4"}, **private).status_code == 403   # edge header still refused


def test_private_listener_serves_nothing_else(env):
    c = smoke_client()
    private = {"environ_overrides": {"SERVER_PORT": "8081"}}
    for path in ("/healthz", "/miniapp", "/", "/api/anything", PREFIX, "/internal/affiliate-v2-report/v1x/summary"):
        assert c.get(path, headers=AUTH, **private).status_code == 404, path
    assert c.get("/healthz", environ_overrides={"SERVER_PORT": "8080"}).get_data() == b"ok\n"
    assert c.get("/miniapp", environ_overrides={"SERVER_PORT": "8080"}).status_code == 200


def test_without_a_private_port_behaviour_is_the_previous_edge_header_rule(env, monkeypatch):
    monkeypatch.delenv("AFFILIATE_V2_REPORT_PRIVATE_PORT")
    c = smoke_client()
    assert c.get(URL, headers=AUTH).status_code == 200
    assert c.get(URL, headers={**AUTH, "Fly-Client-IP": "1.2.3.4"}).status_code == 403


def test_the_guard_is_inert_when_no_private_port_is_configured(monkeypatch):
    monkeypatch.delenv("AFFILIATE_V2_REPORT_PRIVATE_PORT", raising=False)
    app = Flask(__name__)
    api.install_private_listener_guard(app)

    @app.route("/x")
    def x():
        return "x"

    assert app.test_client().get("/x", environ_overrides={"SERVER_PORT": "8081"}).status_code == 200


# --- real gunicorn listeners -------------------------------------------------------------

def get(url, headers=None):
    request = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(request, timeout=5) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


# Simulates a container whose kernel has no IPv6, on any host: AF_INET6 sockets cannot be created.
_NO_IPV6_LAUNCHER = """
import socket, sys
_real = socket.socket
class _NoV6(_real):
    def __init__(self, family=-1, *args, **kwargs):
        if family == socket.AF_INET6:
            raise OSError(97, "Address family not supported by protocol")
        super().__init__(family, *args, **kwargs)
socket.socket = _NoV6
from gunicorn.app.wsgiapp import run
sys.argv[0] = "gunicorn"
run()
"""


class Gunicorn:
    def __init__(self, extra_env, no_ipv6=False):
        self.public, self.private = free_port(), free_port()
        env = {**os.environ, "PYTHONPATH": str(ROOT), "WEB_PUBLIC_BIND": f"127.0.0.1:{self.public}",
               "AFFILIATE_V2_REPORT_ENABLED": "true", "AFFILIATE_V2_REPORT_TOKEN": TOKEN,
               "AFFILIATE_V2_REPORT_PRIVATE_PORT": str(self.private), **extra_env}
        env.pop("DATABOT_API_KEY", None)
        args = ["scripts.listener_smoke_app:app", "-c", "gunicorn_web.conf.py",
                "--workers", "1", "--threads", "2", "--access-logfile", "-", "--error-logfile", "-"]
        launcher = [sys.executable, "-c", _NO_IPV6_LAUNCHER] if no_ipv6 else [sys.executable, "-m", "gunicorn"]
        self.proc = subprocess.Popen(
            [*launcher, *args],
            cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    def wait_ready(self):
        deadline = time.time() + 30
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise AssertionError("gunicorn exited:\n" + self.proc.stdout.read())
            try:
                if get(f"http://127.0.0.1:{self.public}/healthz")[0] == 200:
                    return
            except OSError:
                time.sleep(0.2)
        raise AssertionError("gunicorn never became ready")

    def stop(self):
        self.proc.terminate()
        try:
            return self.proc.communicate(timeout=10)[0]
        except subprocess.TimeoutExpired:
            self.proc.kill()
            return self.proc.communicate()[0]


@pytest.fixture
def server():
    started = []

    def start(extra_env=None, no_ipv6=False):
        g = Gunicorn(extra_env or {}, no_ipv6=no_ipv6)
        started.append(g)
        g.wait_ready()
        return g

    yield start
    for g in started:
        g.stop()


def test_public_listener_and_health_check_work_with_the_private_listener_configured(server):
    require_ipv6()
    g = server()
    status, body = get(f"http://127.0.0.1:{g.public}/healthz")           # IPv4 public listener, unchanged
    assert (status, body) == (200, b"ok\n")
    assert get(f"http://127.0.0.1:{g.public}/miniapp")[0] == 200
    # The report is not usable through the public listener, with or without the token.
    assert get(f"http://127.0.0.1:{g.public}{URL}", AUTH)[0] == 404
    assert get(f"http://127.0.0.1:{g.public}{URL}")[0] == 404


def test_private_ipv6_listener_serves_only_the_authenticated_report(server):
    require_ipv6()
    g = server()
    base = f"http://[::1]:{g.private}"
    assert get(f"{base}{URL}")[0] == 401
    assert get(f"{base}{URL}", {"Authorization": "Bearer wrong"})[0] == 401
    assert get(f"{base}{URL}", {**AUTH, "Fly-Client-IP": "9.9.9.9"})[0] == 403
    status, body = get(f"{base}{URL}", AUTH)
    assert status == 200 and b'"report_mode":"preview"' in body.replace(b" ", b"") and b'"effects_written":false' in body.replace(b" ", b"")
    for path in ("/healthz", "/miniapp", "/"):
        assert get(f"{base}{path}", AUTH)[0] == 404, path


def test_private_listener_is_skipped_not_fatal_when_the_flag_is_off(server):
    g = server({"AFFILIATE_V2_REPORT_ENABLED": "false"})
    assert get(f"http://127.0.0.1:{g.public}/healthz") == (200, b"ok\n")
    with socket.socket(socket.AF_INET) as s:
        s.settimeout(1)
        assert s.connect_ex(("127.0.0.1", g.private)) != 0           # nothing listens on the private port
    assert get(f"http://127.0.0.1:{g.public}{URL}", AUTH)[0] == 404


def test_container_without_ipv6_still_serves_public_and_health(server):
    g = server(no_ipv6=True)
    assert get(f"http://127.0.0.1:{g.public}/healthz") == (200, b"ok\n")
    assert get(f"http://127.0.0.1:{g.public}{URL}", AUTH)[0] == 404
    with socket.socket(socket.AF_INET) as s:
        s.settimeout(1)
        assert s.connect_ex(("127.0.0.1", g.private)) != 0
    assert "IPv6 is not available" in g.stop()
