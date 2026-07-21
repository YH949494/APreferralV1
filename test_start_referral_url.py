import ast
from pathlib import Path
from urllib.parse import parse_qs, urlparse


def _load_webapp_url_constants(miniapp_version="v123"):
    """Extract just the WEBAPP_URL / REFERRAL_WEBAPP_URL assignment lines from
    main.py (via AST, matching the pattern used elsewhere in this test suite)
    so we don't have to import main.py itself (which has heavy side effects
    at import time)."""
    source = Path("main.py").read_text(encoding="utf-8")
    module = ast.parse(source)

    wanted = {"BASE_WEBAPP_URL", "WEBAPP_URL", "REFERRAL_WEBAPP_URL"}
    nodes = [
        node
        for node in module.body
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id in wanted
    ]
    assert {n.targets[0].id for n in nodes} == wanted, "expected WEBAPP_URL constants not found in main.py"

    isolated = ast.Module(body=nodes, type_ignores=[])
    ast.fix_missing_locations(isolated)
    env = {"MINIAPP_VERSION": miniapp_version}
    exec(compile(isolated, filename="main.py", mode="exec"), env)  # noqa: S102
    return env["WEBAPP_URL"], env["REFERRAL_WEBAPP_URL"]


def test_referral_webapp_url_has_exactly_one_query_string():
    webapp_url, referral_url = _load_webapp_url_constants()

    assert webapp_url.count("?") == 1
    assert referral_url.count("?") == 1, "appending action must not introduce a second '?'"


def test_referral_webapp_url_appends_action_without_overwriting_version():
    webapp_url, referral_url = _load_webapp_url_constants(miniapp_version="abc999")

    base, _, query = webapp_url.partition("?")
    referral_base, _, referral_query = referral_url.partition("?")

    assert referral_base == base, "referral URL must point at the same miniapp path"

    params = parse_qs(query)
    referral_params = parse_qs(referral_query)

    assert params["v"] == ["abc999"]
    assert referral_params["v"] == ["abc999"], "existing v= query param must survive unchanged"
    assert referral_params["action"] == ["generate_referral"]


def test_referral_webapp_url_is_a_superset_of_webapp_url_query():
    webapp_url, referral_url = _load_webapp_url_constants()

    parsed_webapp = urlparse(webapp_url)
    parsed_referral = urlparse(referral_url)

    assert parsed_webapp.netloc == parsed_referral.netloc
    assert parsed_webapp.path == parsed_referral.path

    webapp_params = parse_qs(parsed_webapp.query)
    referral_params = parse_qs(parsed_referral.query)

    for key, value in webapp_params.items():
        assert referral_params.get(key) == value
