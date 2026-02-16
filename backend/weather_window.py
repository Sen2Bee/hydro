from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo


def compute_window_safe(hours: int = 552, days_ago: int = 3, tz: str = "Europe/Berlin") -> tuple[str, str]:
    """
    Return (startISO, endISO) in UTC.

    "Safe" means: end is in the past (start of day in local TZ minus days_ago).
    This avoids requesting partial/incomplete "today" ranges.
    """
    h = int(hours) if isinstance(hours, int | float) else 552
    if h <= 0:
        h = 552
    d = int(days_ago) if isinstance(days_ago, int | float) else 3
    if d < 0:
        d = 0

    zone = ZoneInfo(tz)
    now_local = dt.datetime.now(tz=zone)
    end_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0) - dt.timedelta(days=d)
    end_utc = end_local.astimezone(dt.timezone.utc)
    start_utc = end_utc - dt.timedelta(hours=h)
    return start_utc.isoformat().replace("+00:00", "Z"), end_utc.isoformat().replace("+00:00", "Z")

