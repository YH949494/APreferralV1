"""Import helper: load ``gunicorn_web.conf.py`` (not importable by name)."""
import importlib.util
from pathlib import Path


def load():
    path = Path(__file__).resolve().parent / "gunicorn_web.conf.py"
    spec = importlib.util.spec_from_file_location("gunicorn_web_conf", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
