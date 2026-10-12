# XP startup recovery

## Problem and scope

`main.ensure_indexes()` calls `xp.ensure_xp_indexes()` during every worker
startup. Previously the latter aggregated all XP events and deleted duplicates
before declaring indexes, even when the unique index already existed. Concurrent
web boots repeated this expensive, destructive work. During the October 12
incident, skipping the scan after verifying the unique index and reloading each
web machine restored local `/miniapp` responses to HTTP 200.

The patch removes automatic XP history repair and the unused duplicate-repair
helper in `main.py`. It retains all XP index definitions and grant idempotency.
A unique-index build that encounters genuine duplicates still raises; it does
not silently delete records or weaken uniqueness. Resolve such a failure with
an explicitly reviewed maintenance procedure, not a startup cleanup.

Fly now checks `GET /healthz` for HTTP 200 on the web service. The route performs
no MongoDB work, needs no login, does not redirect, and disables caching. A pass
proves a web worker can answer after application initialization; it does not
prove every miniapp API, database dependency, or Telegram authentication works.
The check does not restart crashed workers by itself.

## Release and verification

Creating this PR does not deploy. Merging to `main` triggers the existing Fly
deployment workflow. Qualification activation, cutoff, account policies, and
rewards are untouched.

After an authorized deploy, run in PowerShell:

```powershell
fly status -a apreferralv1
fly checks list -a apreferralv1
curl.exe -sS -o NUL -w "HTTP %{http_code}\n" --max-time 20 https://apreferralv1.fly.dev/healthz
curl.exe -sS -L -o NUL -w "HTTP %{http_code}\n" --max-time 20 https://apreferralv1.fly.dev/miniapp
```

Require passing web checks and HTTP 200. Open the miniapp through Telegram and
verify its data loads; shell HTTP checks alone are not complete user-flow tests.
Watch both web machines for another timeout/restart cycle. If local HTTP passes
but public requests still fail, inspect Fly routing, cordon state and the actual
failed URL; do not declare every public failure to be an XP scan.

The prior in-machine edits are temporary and will be replaced by deployment;
this repository patch makes the startup change durable. No XP data migration,
index removal, or production data repair is needed for the reported machines,
whose required unique XP index was already verified by the recovery script.
