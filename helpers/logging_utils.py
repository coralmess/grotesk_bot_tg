from __future__ import annotations

import logging
import re

_TELEGRAM_API_URL_RE = re.compile(r"(https://api\.telegram\.org/bot)([^/\s]+)")
_TELEGRAM_TOKEN_RE = re.compile(r"\b(\d{6,}):([A-Za-z0-9_-]{20,})\b")


def redact_secrets(text: str) -> str:
    value = text or ""
    value = _TELEGRAM_API_URL_RE.sub(r"\1<redacted>", value)
    value = _TELEGRAM_TOKEN_RE.sub(r"\1:<redacted>", value)
    return value


class SecretRedactingFilter(logging.Filter):
    """Redact common bot secrets before records reach handlers."""

    def filter(self, record: logging.LogRecord) -> bool:  # pragma: no cover - tiny wrapper
        try:
            rendered = record.getMessage()
        except Exception:
            return True
        record.msg = redact_secrets(rendered)
        record.args = ()
        return True


def configure_third_party_loggers() -> None:
    """Keep noisy request libraries quiet unless they emit actual warnings/errors."""
    for logger_name in (
        "httpx",
        "httpcore",
        "telegram",
        "telegram.ext",
        "telegram.request",
        "urllib3",
    ):
        logging.getLogger(logger_name).setLevel(logging.WARNING)


def install_secret_redaction(target_logger: logging.Logger) -> None:
    """Attach a single shared redaction filter to the logger and its handlers."""
    already_installed = any(isinstance(item, SecretRedactingFilter) for item in target_logger.filters)
    if not already_installed:
        target_logger.addFilter(SecretRedactingFilter())
    for handler in target_logger.handlers:
        if not any(isinstance(item, SecretRedactingFilter) for item in handler.filters):
            handler.addFilter(SecretRedactingFilter())
