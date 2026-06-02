"""GitHub API URL normalization for the search wrapper."""

from __future__ import annotations

from urllib.parse import urlparse


DEFAULT_API_BASE_URL = "https://api.github.com"
DEFAULT_INSTANCE = "github_com"


def normalize_api_base_url(api_url_base: str | None = None) -> str:
    """Validate and normalize a GitHub API base URL before requests use it.

    This intentionally validates URL shape only. Deployment-specific host policy
    belongs in the caller's configuration, not in this reusable tap package.
    """
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
    if parsed.query or parsed.fragment or parsed.params:
        raise ValueError(f"api_url_base {raw_url!r} must not include query or fragment")

    return f"https://{host}{parsed.path.rstrip('/')}"
