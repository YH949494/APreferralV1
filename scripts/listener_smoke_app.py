"""Minimal WSGI app for listener smoke tests (never used in production).

The real ``main:app`` needs a live MongoDB at import time, which a listener
smoke must not depend on. This app wires the *real* pieces under test - the
V2 report blueprint with its authentication, the private-listener guard - plus
a ``/healthz`` that matches ``main.web_readiness`` byte for byte, over a stub
database that answers "nothing stored" (a never-activated, no-snapshot system).

    gunicorn scripts.listener_smoke_app:app -c gunicorn_web.conf.py
"""
from flask import Flask, make_response

import affiliate_v2_report_api as api


class _Collection:
    def find_one(self, *args, **kwargs):
        return None

    def find(self, *args, **kwargs):
        return []

    def count_documents(self, *args, **kwargs):
        return 0


class StubDb:
    def __getitem__(self, name):
        return _Collection()

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        return _Collection()


app = Flask(__name__)
api.install_private_listener_guard(app)
app.register_blueprint(api.make_blueprint(lambda: StubDb()))


@app.route("/healthz", methods=["GET"])
def web_readiness():
    response = make_response("ok\n", 200)
    response.headers["Cache-Control"] = "no-store"
    return response


@app.route("/miniapp")
def stand_in_for_the_rest_of_the_app():
    return "public app\n"
