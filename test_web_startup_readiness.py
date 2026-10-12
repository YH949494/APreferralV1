"""Readiness must be HTTP 200 without redirects, authentication or DB work."""
import ast
from pathlib import Path

from flask import Flask, make_response
import re


ROOT = Path(__file__).resolve().parent


def test_fly_readiness_route_returns_200_without_database():
    # Extract the actual endpoint without importing production DB startup.
    tree = ast.parse((ROOT / "main.py").read_text())
    route = next(node for node in tree.body
                 if isinstance(node, ast.FunctionDef) and node.name == "web_readiness")
    app = Flask(__name__)
    scope = {"app": app, "make_response": make_response}
    exec(compile(ast.Module(body=[route], type_ignores=[]), "main.py", "exec"), scope)
    response = app.test_client().get("/healthz")
    assert response.status_code == 200
    assert response.data == b"ok\n"
    assert response.headers["Cache-Control"] == "no-store"
    config = (ROOT / "fly.toml").read_text()
    service = config.split("[[services]]", 1)[1]
    assert 'processes = ["web"]' in service.split("[[services.", 1)[0]
    check = re.search(r"\[\[services\.http_checks\]\]([^\[]+)", service).group(1)
    assert 'path = "/healthz"' in check
    assert 'method = "GET"' in check
