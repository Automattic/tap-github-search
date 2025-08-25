from __future__ import annotations

from typing import Any

import requests

from tap_github.authenticator import (
    GitHubTokenAuthenticator,
    TokenManager,
)


class GHEPersonalTokenManager(TokenManager):
    """TokenManager that validates against a given API base URL (GHE-aware)."""

    def __init__(self, token: str, api_base_url: str, rate_limit_buffer: int | None = None, logger: Any | None = None) -> None:
        super().__init__(token, rate_limit_buffer=rate_limit_buffer, logger=logger)
        self.api_base_url = api_base_url.rstrip("/")

    def is_valid_token(self) -> bool:  # type: ignore[override]
        if not self.token:
            return False
        try:
            response = requests.get(
                url=f"{self.api_base_url}/rate_limit",
                headers={"Authorization": f"token {self.token}"},
            )
            response.raise_for_status()
            return True
        except requests.exceptions.HTTPError:
            msg = (
                f"A token could not be validated. "
                f"{response.status_code} Client Error: "
                f"{response.content!s} (Reason: {response.reason})"
            )
            if self.logger is not None:
                self.logger.warning(msg)
            return False
        except Exception:
            if self.logger is not None:
                self.logger.warning("Token validation failed due to connection error.")
            return False


class WrapperGitHubTokenAuthenticator(GitHubTokenAuthenticator):
    """Authenticator that validates tokens against the configured API base URL.

    This avoids rejecting valid GitHub Enterprise tokens by checking
    the instance's own /rate_limit endpoint instead of api.github.com.
    """

    def __init__(self, stream) -> None:  # noqa: ANN001
        self._api_base_url = self._extract_api_base_url_from_stream(stream)
        super().__init__(stream=stream)

    @staticmethod
    def _extract_api_base_url_from_stream(stream) -> str:  # noqa: ANN001
        # Default to public GitHub
        api_base_url = "https://api.github.com"
        try:
            # Prefer search config injected by wrapper tap
            cfg = getattr(stream, "_search_cfg", {}) or {}
            if cfg:
                api_base_url = (
                    cfg.get("search", {})
                    .get("scope", {})
                    .get("api_url_base", api_base_url)
                )
            else:
                # Fallback to stream config if present
                api_base_url = (
                    stream.config.get("search", {})
                    .get("scope", {})
                    .get("api_url_base", api_base_url)
                )
        except Exception:
            pass
        return api_base_url.rstrip("/")

    def prepare_tokens(self) -> list[TokenManager]:  # type: ignore[override]
        env_dict = self.get_env()
        rate_limit_buffer = self._config.get("rate_limit_buffer", None)
        expiry_time_buffer = self._config.get("expiry_time_buffer", None)

        # Collect personal tokens from config and env
        personal_tokens: set[str] = set()
        if "auth_token" in self._config:
            personal_tokens.add(self._config["auth_token"])
        if "additional_auth_tokens" in self._config:
            personal_tokens = personal_tokens.union(self._config["additional_auth_tokens"])
        else:
            env_tokens = {value for key, value in env_dict.items() if key.startswith("GITHUB_TOKEN")}
            if len(env_tokens) > 0:
                self.logger.info(
                    f"Found {len(env_tokens)} 'GITHUB_TOKEN' environment variables for authentication."
                )
                personal_tokens = personal_tokens.union(env_tokens)

        personal_token_managers: list[TokenManager] = []
        for token in personal_tokens:
            token_manager = GHEPersonalTokenManager(
                token,
                api_base_url=self._api_base_url,
                rate_limit_buffer=rate_limit_buffer,
                logger=self.logger,
            )
            if token_manager.is_valid_token():
                personal_token_managers.append(token_manager)
            else:
                self.logger.warning("A token was dismissed.")

        # App tokens: fall back to parent behavior which contacts api.github.com
        # This is acceptable for now; can be extended if needed for GHE app tokens.
        app_token_managers: list[TokenManager] = []
        try:
            app_token_managers = super().prepare_tokens()[len(personal_token_managers) :]
        except Exception:
            pass

        self.logger.info(
            f"Tap will run with {len(personal_token_managers)} personal auth tokens "
            f"and {len(app_token_managers)} app keys."
        )
        return personal_token_managers + app_token_managers
