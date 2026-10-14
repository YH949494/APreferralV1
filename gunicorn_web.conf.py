"""Gunicorn settings for the Fly ``web`` process (see fly.toml).

Public listener: ``0.0.0.0:8080`` - exactly what the process used before
(Fly edge ``[[services]]`` port 80/443 -> 8080 and the ``/healthz`` check).
Nothing here changes it, and it is always bound first.

Optional private listener for Databot's V2 report reads: ``[::]:<port>`` on
Fly's IPv6 private network (``apreferralv1.internal``). It exists only when BOTH

    AFFILIATE_V2_REPORT_ENABLED=true
    AFFILIATE_V2_REPORT_PRIVATE_PORT=<1024-65535, not 8080>

are set, so a deploy without those variables is byte-for-byte the old
behaviour. If the container has no IPv6 the private listener is skipped with a
warning rather than preventing the web process from booting - the public
listener and health check must never depend on it. ``fly.toml`` maps only 8080
to the Fly edge, so the private port is unreachable from the internet; the
application additionally serves the report route only on that port and
nothing else on it (affiliate_v2_report_api).

``WEB_PUBLIC_BIND`` exists for the listener smoke tests; production leaves it
unset.
"""
import logging
import os
import socket

PUBLIC_BIND_DEFAULT = "0.0.0.0:8080"
log = logging.getLogger("gunicorn.error")


def ipv6_available() -> bool:
    try:
        probe = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    except OSError:
        return False
    try:
        probe.bind(("::", 0))
        return True
    except OSError:
        return False
    finally:
        probe.close()


def build_bind(env=None, ipv6_ok=ipv6_available) -> list[str]:
    env = os.environ if env is None else env
    public = env.get("WEB_PUBLIC_BIND", "").strip() or PUBLIC_BIND_DEFAULT
    binds = [public]
    if env.get("AFFILIATE_V2_REPORT_ENABLED", "").strip().lower() != "true":
        return binds
    port = env.get("AFFILIATE_V2_REPORT_PRIVATE_PORT", "").strip()
    if not port:
        return binds
    if not (port.isdigit() and 1024 <= int(port) <= 65535) or public.rsplit(":", 1)[-1] == port:
        log.warning("[V2_REPORT] private listener not started: invalid or conflicting AFFILIATE_V2_REPORT_PRIVATE_PORT")
        return binds
    if not ipv6_ok():
        log.warning("[V2_REPORT] private listener not started: IPv6 is not available in this container")
        return binds
    binds.append(f"[::]:{port}")
    return binds


bind = build_bind()
