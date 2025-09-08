from __future__ import annotations
import base64
import json
import os
from singer_sdk import Stream
from tap_github.tap import TapGitHub
from tap_github_search.search_count_streams import (
    create_configurable_streams,
    ConfigurableSearchCountStream,
)


class TapGitHubSearch(TapGitHub):
    name = "tap-github-search"

    def discover_streams(self) -> list[Stream]:
        search_cfg_b64 = os.environ.get("TAP_GITHUB_SEARCH_STATS_SEARCH_B64")
        search_cfg = os.environ.get("GITHUB_SEARCH_CONFIG")
        
        if search_cfg_b64:
            try:
                search_cfg = base64.b64decode(search_cfg_b64).decode("utf-8")
            except Exception as e:
                raise ValueError(f"Failed to decode TAP_GITHUB_SEARCH_STATS_SEARCH_B64: {e}")
        
        if not search_cfg and "search" not in self.config:
            raise ValueError("Provide search.* in config, set GITHUB_SEARCH_CONFIG, or set TAP_GITHUB_SEARCH_STATS_SEARCH_B64.")

        cfg = dict(self.config)
        if "search" not in cfg and search_cfg:
            cfg["search"] = json.loads(search_cfg)

        streams = create_configurable_streams(self, config_override=cfg)
        for s in streams:
            if isinstance(s, ConfigurableSearchCountStream):
                setattr(s, "_search_cfg", {"search": cfg["search"]})
        return streams


cli = TapGitHubSearch.cli
