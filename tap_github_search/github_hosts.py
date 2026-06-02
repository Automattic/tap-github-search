"""GitHub API URL defaults for the search wrapper."""

from __future__ import annotations


DEFAULT_API_BASE_URL = "https://api.github.com"
DEFAULT_INSTANCE = "github_com"


def normalize_api_base_url(api_url_base: str | None = None) -> str:
    """Return a trimmed API base URL, defaulting to public GitHub."""
    if api_url_base is None:
        return DEFAULT_API_BASE_URL
    return api_url_base.strip().rstrip("/")
