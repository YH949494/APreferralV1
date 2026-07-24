import os
import re

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))


def test_hero_band_image_resolves_through_static_path():
    index_path = os.path.join(REPO_ROOT, "static", "index.html")
    with open(index_path, encoding="utf-8") as f:
        html = f.read()

    # The hero image must be referenced via the real /static/... route, not a
    # relative path that resolves against whatever page served index.html
    # (which previously 404'd at /redesign/hero-band.jpg).
    assert 'src="redesign/hero-band.jpg"' not in html
    assert 'src="/static/redesign/hero-band.jpg"' in html

    asset_path = os.path.join(REPO_ROOT, "static", "redesign", "hero-band.jpg")
    assert os.path.isfile(asset_path)


def test_gunicorn_access_log_format_excludes_query_strings():
    fly_toml_path = os.path.join(REPO_ROOT, "fly.toml")
    with open(fly_toml_path, encoding="utf-8") as f:
        fly_toml = f.read()

    match = re.search(r"--access-logformat '([^']*)'", fly_toml)
    assert match, "expected --access-logformat argument in fly.toml web process"
    fmt = match.group(1)

    # %(U)s is the URL path only; %(r)s (full request line) and %(q)s (raw
    # query string) would leak Telegram init_data, hashes, and tokens.
    assert "%(U)s" in fmt
    assert "%(r)s" not in fmt
    assert "%(q)s" not in fmt

    for atom in ("%(h)s", "%(t)s", "%(m)s", "%(H)s", "%(s)s", "%(b)s", "%(L)s"):
        assert atom in fmt

    # Access logging must remain enabled (not disabled/discarded).
    assert "--access-logfile -" in fly_toml
    assert "--access-logfile /dev/null" not in fly_toml
