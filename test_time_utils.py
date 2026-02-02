from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytz

from time_utils import expires_in_seconds, tz_name


def test_tz_name_zoneinfo_and_pytz():
    assert tz_name(ZoneInfo("Asia/Kuala_Lumpur")) == "Asia/Kuala_Lumpur"
    assert tz_name(pytz.UTC) == "UTC"


def test_expires_in_seconds_with_aware_times():
    now = datetime.now(timezone.utc)
    expire_at = now + timedelta(seconds=60)
    remaining = expires_in_seconds(expire_at, now=now)
    assert remaining == 60.0
