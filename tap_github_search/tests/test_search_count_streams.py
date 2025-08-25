from __future__ import annotations

from unittest.mock import Mock

from tap_github_search.search_count_streams import (
    ConfigurableSearchCountStream,
    create_configurable_streams,
    validate_stream_config,
)
from tap_github_search.tap import TapGitHubSearch


def test_validate_stream_config_minimal():
    good = {"name": "issues", "query_template": "org:{org} type:issue created:{start}..{end}"}
    bad = {"name": "issues"}

    assert validate_stream_config(good) == []
    errs = validate_stream_config(bad)
    assert any("query_template" in e for e in errs)


def test_create_streams_from_search_namespace():
    cfg = {
        "search": {
            "streams": [
                {
                    "name": "issues",
                    "query_template": "org:{org} type:issue is:open created:{start}..{end}",
                }
            ],
            "scope": {"api_url_base": "https://api.github.com", "orgs": ["Automattic"], "breakdown": "none"},
            "backfill": {"start_month": "2025-01", "end_month": "2025-01"},
        }
    }
    tap = TapGitHubSearch(config=cfg)
    streams = tap.discover_streams()
    assert len(streams) == 1
    assert streams[0].name == "issues_search_counts"


def test_query_template_substitution():
    stream_config = {
        "name": "test",
        "query_template": "org:{org} type:issue label:test created:{start}..{end}",
    }
    mock_tap = Mock()
    mock_tap.config = {}
    stream = ConfigurableSearchCountStream(stream_config, mock_tap)

    q = stream._build_search_query("WordPress", "2025-01-01", "2025-01-31", "issues")
    assert q == "org:WordPress type:issue label:test created:2025-01-01..2025-01-31"
