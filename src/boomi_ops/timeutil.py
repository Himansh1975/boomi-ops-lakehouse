"""Boomi timestamps look like 2026-08-30T14:22:10Z. Everything here is UTC."""
from datetime import datetime, timezone


def to_boomi(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def from_boomi(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def parse_cli(text: str) -> datetime:
    """Accepts 2026-11-01 or 2026-11-01T10:00 (or with Z); returns aware UTC."""
    text = text.rstrip("Z")
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    raise ValueError(f"not a date/time: {text}")
