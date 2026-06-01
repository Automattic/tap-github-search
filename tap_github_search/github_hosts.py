"""Trusted GitHub API hosts for the search wrapper."""

from __future__ import annotations

from urllib.parse import urlparse


DEFAULT_API_BASE_URL = "https://api.github.com"

VALID_API_HOSTS = frozenset({
    "api.github.com",
    "github.a8c.com",
    "github.tumblr.net",
})

INSTANCE_BY_API_HOST = {
    "api.github.com": "github_com",
    "github.a8c.com": "a8c_ghe",
    "github.tumblr.net": "tumblr_ghe",
}


def normalize_api_base_url(api_url_base: str | None = None) -> str:
    """Validate and normalize an API base URL before any request is built."""
    raw_url = (api_url_base or DEFAULT_API_BASE_URL).strip().rstrip("/")
    parsed = urlparse(raw_url)
    host = (parsed.hostname or "").lower()

    if parsed.scheme != "https":
        raise ValueError(
            f"api_url_base {raw_url!r} must use https "
            f"(got scheme={parsed.scheme!r})"
        )
    if not host:
        raise ValueError(f"api_url_base {raw_url!r} has no hostname")
    if parsed.username or parsed.password:
        raise ValueError(f"api_url_base {raw_url!r} must not include credentials")
    if parsed.port not in (None, 443):
        raise ValueError(f"api_url_base {raw_url!r} must not include a non-443 port")
    if host not in VALID_API_HOSTS:
        raise ValueError(
            f"api_url_base host {host!r} not in allowlist "
            f"{sorted(VALID_API_HOSTS)}; add the host if new"
        )
    if parsed.query or parsed.fragment or parsed.params:
        raise ValueError(f"api_url_base {raw_url!r} must not include query or fragment")

    return f"https://{host}{parsed.path.rstrip('/')}"


def instance_from_api_base(api_url_base: str | None = None) -> str:
    """Return the reporting instance for a trusted GitHub API base URL."""
    if api_url_base == "":
        return "unknown"
    try:
        normalized = normalize_api_base_url(api_url_base)
    except ValueError:
        return "unknown"
    host = (urlparse(normalized).hostname or "").lower()
    return INSTANCE_BY_API_HOST.get(host, "unknown")
