"""Avatar security, validation, fetching, and fallback for WeChat Hub Core.

Implements E4 Avatar Security:
- MIME validation (only image/jpeg, image/png, image/webp, image/gif)
- Max size limit (2MB)
- Request timeout (3.0s)
- Server-side caching (avatar_cache)
- Anti-SSRF allowlist for WeChat CDN hosts
- Safe fallbacks without leaking arbitrary external requests
"""

from __future__ import annotations

import ipaddress
import mimetypes
import re
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


MAX_AVATAR_BYTES = 2 * 1024 * 1024  # 2MB
DEFAULT_AVATAR_TIMEOUT = 3.0  # 3.0 seconds

ALLOWED_IMAGE_MIMES = {
    "image/jpeg",
    "image/png",
    "image/webp",
    "image/gif",
}

ALLOWED_AVATAR_HOST_PATTERNS = (
    re.compile(r"^([a-zA-Z0-9\-]+\.)?qlogo\.cn\Z", re.IGNORECASE),
    re.compile(r"^([a-zA-Z0-9\-]+\.)?qpic\.cn\Z", re.IGNORECASE),
    re.compile(r"^([a-zA-Z0-9\-]+\.)?qq\.com\Z", re.IGNORECASE),
    re.compile(r"^([a-zA-Z0-9\-]+\.)?wechat\.com\Z", re.IGNORECASE),
    re.compile(r"^([a-zA-Z0-9\-]+\.)?weixin\.qq\.com\Z", re.IGNORECASE),
)


class AvatarSecurityError(RuntimeError):
    """Security or network exception when fetching/validating avatar."""

    def __init__(self, code: str, status: int, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.status = status
        self.message = message
        self.details = details or {}


def detect_image_mime(data: bytes) -> str | None:
    """Inspect binary magic bytes to determine image MIME type."""
    if not data or len(data) < 4:
        return None
    if data.startswith(b"\x89PNG\r\n\x1a\n") or data.startswith(b"\x89PNG"):
        return "image/png"
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith(b"GIF87a") or data.startswith(b"GIF89a"):
        return "image/gif"
    if data.startswith(b"RIFF") and len(data) >= 12 and data[8:12] == b"WEBP":
        return "image/webp"
    return None


def validate_avatar_url(url: str) -> None:
    """Verify avatar URL against scheme, host allowlist, and anti-SSRF rules."""
    if not url or not isinstance(url, str):
        raise AvatarSecurityError("invalid_url", 400, "Avatar URL must be a non-empty string")
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme.lower() not in {"http", "https"}:
        raise AvatarSecurityError("invalid_scheme", 400, f"Unsupported scheme: {parsed.scheme}")

    hostname = (parsed.hostname or "").strip().lower()
    if not hostname:
        raise AvatarSecurityError("missing_host", 400, "Avatar URL lacks hostname")

    # Check for IP address (SSRF prevention)
    try:
        ip = ipaddress.ip_address(hostname)
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast:
            raise AvatarSecurityError("ssrf_blocked", 403, f"Private/reserved IP address forbidden: {hostname}")
        raise AvatarSecurityError("ssrf_blocked", 403, f"Direct IP avatar URLs forbidden: {hostname}")
    except ValueError:
        pass  # Hostname is a domain name, not an IP

    if hostname in {"localhost", "localhost.localdomain"}:
        raise AvatarSecurityError("ssrf_blocked", 403, "Localhost forbidden")

    # Check allowlist
    allowed = any(pat.match(hostname) for pat in ALLOWED_AVATAR_HOST_PATTERNS)
    if not allowed:
        raise AvatarSecurityError(
            "ssrf_blocked",
            403,
            f"Host {hostname} not permitted by WeChat avatar allowlist",
            {"host": hostname},
        )


def fetch_remote_avatar(
    url: str,
    *,
    timeout: float = DEFAULT_AVATAR_TIMEOUT,
    max_bytes: int = MAX_AVATAR_BYTES,
) -> tuple[bytes, str]:
    """Safely fetch an avatar from a validated remote WeChat CDN URL.

    Enforces SSRF allowlist, timeout, size limit, and MIME detection.
    """
    validate_avatar_url(url)

    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "WeChatHubCore/1.0",
            "Accept": "image/jpeg,image/png,image/webp,image/*;q=0.8",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as resp:
            content_length_hdr = resp.headers.get("Content-Length")
            if content_length_hdr:
                try:
                    content_length = int(content_length_hdr)
                    if content_length > max_bytes:
                        raise AvatarSecurityError(
                            "oversize",
                            413,
                            f"Avatar Content-Length {content_length} exceeds limit {max_bytes}",
                            {"size": content_length, "limit": max_bytes},
                        )
                except ValueError:
                    pass

            # Read in chunks to strictly limit size
            chunks: list[bytes] = []
            total_read = 0
            chunk_size = 64 * 1024
            while True:
                chunk = resp.read(chunk_size)
                if not chunk:
                    break
                chunks.append(chunk)
                total_read += len(chunk)
                if total_read > max_bytes:
                    raise AvatarSecurityError(
                        "oversize",
                        413,
                        f"Avatar downloaded bytes {total_read} exceeds limit {max_bytes}",
                        {"size": total_read, "limit": max_bytes},
                    )
            body = b"".join(chunks)
    except TimeoutError as exc:
        raise AvatarSecurityError("fetch_timeout", 504, f"Avatar fetch timed out: {exc}") from exc
    except urllib.error.URLError as exc:
        if isinstance(exc.reason, TimeoutError) or "timed out" in str(exc.reason).lower():
            raise AvatarSecurityError("fetch_timeout", 504, f"Avatar fetch timed out: {exc.reason}") from exc
        raise AvatarSecurityError("fetch_failed", 502, f"Avatar fetch failed: {exc}") from exc
    except OSError as exc:
        if "timed out" in str(exc).lower():
            raise AvatarSecurityError("fetch_timeout", 504, f"Avatar fetch timed out: {exc}") from exc
        raise AvatarSecurityError("fetch_failed", 502, f"Avatar fetch failed: {exc}") from exc

    detected_mime = detect_image_mime(body)
    if not detected_mime or detected_mime not in ALLOWED_IMAGE_MIMES:
        raise AvatarSecurityError(
            "bad_mime",
            415,
            f"Disallowed or unrecognizable image MIME type: {detected_mime or 'unknown'}",
            {"detected_mime": detected_mime},
        )

    return body, detected_mime


def fallback_avatar_svg(name: str = "") -> tuple[bytes, str]:
    """Generate an accessible, neutral inline SVG avatar placeholder."""
    initial = (name or "?").strip()[:1].upper() or "?"
    # Neutral slate background with clean contrasting initial
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64" width="64" height="64">
  <rect width="64" height="64" rx="32" fill="#475569" />
  <text x="32" y="39" font-family="-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif"
        font-size="24" font-weight="600" fill="#f8fafc" text-anchor="middle" dominant-baseline="central">{initial}</text>
</svg>"""
    return svg.encode("utf-8"), "image/svg+xml"
