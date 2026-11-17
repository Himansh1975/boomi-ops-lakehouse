"""Error fingerprints: the same error 40 times should count as one incident.

The message is turned into a template by masking the parts that change from run to run (IDs,
numbers, quoted values, URLs, emails). The template is hashed to get the fingerprint.

The rules are plain regexes that mean the same thing in Python and Java, so Spark can apply them
natively (regexp_replace) and this module can apply them in tests and in the agent. A test checks
that both give the same result.
"""
from __future__ import annotations

import hashlib
import re

# (pattern, replacement), applied in order. HTTP status codes and port numbers are kept on purpose:
# "HTTP 401" and "HTTP 500" are different problems.
RULES: list[tuple[str, str]] = [
    (r"execution-[0-9a-fA-F-]{36}-\d{4}\.\d{2}\.\d{2}", "<exec_id>"),
    (r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", "<uuid>"),
    (r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", "<email>"),
    (r"https?://[^\s'\"]+", "<url>"),
    (r"\"[^\"]*\"", "<str>"),
    (r"(?<!HTTP )(?<![:\d])\b\d+\b", "<n>"),
    (r"\s+", " "),
]
MAX_LEN = 500


def template(message: str | None) -> str:
    text = message or ""
    for pattern, repl in RULES:
        text = re.sub(pattern, repl, text)
    return text.strip()[:MAX_LEN]


def fingerprint(message: str | None) -> str | None:
    t = template(message)
    return hashlib.sha256(t.encode()).hexdigest()[:16] if t else None
