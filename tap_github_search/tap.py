from __future__ import annotations

from singer_sdk import Stream
from tap_github.tap import TapGitHub
from tap_github_search.search_count_streams import (
    _decode_search_config,
    create_configurable_streams,
)


class TapGitHubSearch(TapGitHub):
    name = "tap-github-search"

    def discover_streams(self) -> list[Stream]:
        env_search_config = _decode_search_config()

        if not env_search_config and "search" not in self.config:
            raise ValueError("Provide search.* in config, set TAP_GITHUB_SEARCH_CONFIG, or set TAP_GITHUB_SEARCH_CONFIG_B64.")

        cfg = dict(self.config)
        if env_search_config:
            cfg["search"] = env_search_config

        streams = create_configurable_streams(self, config_override=cfg)
        for s in streams:
            setattr(s, "_search_cfg", {"search": cfg["search"]})
        return streams


cli = TapGitHubSearch.cli
