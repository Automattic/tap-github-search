from __future__ import annotations

import base64
import json
import logging
from unittest.mock import patch

from tap_github_search.github_hosts import DEFAULT_API_BASE_URL
from tap_github_search.search_count_streams import (
    ConfigurablePrVelocityStream,
    NODES_THRESHOLD,
    create_configurable_streams,
    validate_stream_config,
)
from tap_github_search.tap import TapGitHubSearch


class _DummyTap:
    config: dict = {}
    state: dict = {}
    logger = logging.getLogger("dummy_tap")
    metrics_logger = logging.getLogger("dummy_metrics")
    name = "dummy_tap"
    initialized_at = 0
    rate_limit_buffer = 0

    def setup_mapper(self):
        pass


def _mk_velocity(*, markers=None, reviewer="", instance="github_com"):
    stream_config = {
        "name": "pr_velocity",
        "query_template": "org:{org} type:pr is:closed closed:{start}..{end}",
        "mode": "pr_velocity",
        "markers": markers or [],
        "reviewer_clause": reviewer,
    }
    stream = ConfigurablePrVelocityStream(stream_config, _DummyTap())
    stream._search_cfg = {
        "search": {
            "scope": {
                "api_url_base": DEFAULT_API_BASE_URL,
                "orgs": ["example-org"],
                "instance": instance,
            },
            "backfill": {"start_month": "2026-04"},
        }
    }
    return stream


def test_pr_velocity_config_validation():
    errors = validate_stream_config({
        "name": "vel",
        "query_template": "org:{org} type:pr is:closed created:{start}..{end}",
        "mode": "pr_velocity",
    })
    assert any("closed:{start}..{end}" in err for err in errors)
    assert any("Unknown mode" in err for err in validate_stream_config({
        "name": "vel",
        "query_template": "{org} {start} {end}",
        "mode": "pr-velocity",
    }))


def test_dispatches_pr_velocity_mode():
    tap = _DummyTap()
    config = {
        "search": {
            "streams": [{
                "name": "pr_velocity",
                "query_template": "org:{org} type:pr is:closed closed:{start}..{end}",
                "mode": "pr_velocity",
            }],
            "scope": {"api_url_base": DEFAULT_API_BASE_URL, "orgs": ["example-org"]},
            "backfill": {"start_month": "2026-04"},
        }
    }
    streams = create_configurable_streams(tap, config_override=config)
    assert len(streams) == 1
    assert type(streams[0]) is ConfigurablePrVelocityStream


def test_process_window_sets_minimal_fields_and_ai_flags():
    stream = _mk_velocity(
        markers=['"AI marker"', "assistant-marker"],
        reviewer="reviewed-by:review-bot",
    )
    nodes = [
        {
            "number": 1,
            "repository": {"nameWithOwner": "example-org/example-repo", "name": "example-repo"},
            "createdAt": "2026-04-01T00:00:00Z",
            "closedAt": "2026-04-01T01:00:00Z",
            "mergedAt": "2026-04-01T01:00:00Z",
            "author": {"login": "author-one"},
            "bodyText": "Contains the AI marker.",
        },
        {
            "number": 2,
            "repository": {"nameWithOwner": "example-org/example-repo", "name": "example-repo"},
            "createdAt": "2026-04-01T00:00:00Z",
            "closedAt": "2026-04-01T02:00:00Z",
            "mergedAt": None,
            "author": None,
            "bodyText": "Hand-authored PR.",
        },
    ]

    with patch.object(stream, "_iter_pr_nodes", return_value=iter(nodes)), \
         patch.object(stream, "_collect_pr_ids", return_value={"example-org/example-repo#2"}):
        rows = list(stream._process_window(
            "org:example-org type:pr is:closed closed:2026-04-01..2026-04-01",
            DEFAULT_API_BASE_URL,
            "github_com",
            "example-org",
            "2026-04",
            "now",
            stream.stream_config["markers"],
            stream.stream_config["reviewer_clause"],
        ))

    assert set(rows[0]) == {
        "instance", "org_", "repo", "pr_number", "author_login", "created_at",
        "closed_at", "merged_at", "hours_to_close", "outcome",
        "is_ai_authored", "is_ai_reviewed", "month", "synced_at",
    }
    assert rows[0]["org_"] == "example-org"
    assert rows[0]["hours_to_close"] == 1.0
    assert rows[0]["is_ai_authored"] is True
    assert rows[0]["is_ai_reviewed"] is False
    assert rows[1]["outcome"] == "closed_unmerged"
    assert rows[1]["author_login"] is None
    assert rows[1]["is_ai_reviewed"] is True


def test_get_records_day_slices_when_month_exceeds_search_cap():
    stream = _mk_velocity()
    with patch.object(stream, "_search_aggregate_count", side_effect=[NODES_THRESHOLD + 1] + [5] * 30), \
         patch.object(stream, "_process_window", return_value=[]) as fake_process:
        list(stream.get_records({
            "org": "example-org",
            "month": "2026-04",
            "search_query": "org:example-org type:pr is:closed closed:2026-04-01..2026-04-30",
            "api_url_base": DEFAULT_API_BASE_URL,
        }))

    assert fake_process.call_count == 30
    assert "closed:2026-04-01..2026-04-01" in fake_process.call_args_list[0][0][0]


def test_get_records_fails_when_single_day_exceeds_search_cap():
    stream = _mk_velocity()
    with patch.object(stream, "_search_aggregate_count", side_effect=[NODES_THRESHOLD + 1, NODES_THRESHOLD + 1]):
        try:
            list(stream.get_records({
                "org": "example-org",
                "month": "2026-04",
                "search_query": "org:example-org type:pr is:closed closed:2026-04-01..2026-04-30",
                "api_url_base": DEFAULT_API_BASE_URL,
            }))
        except RuntimeError as exc:
            assert "exceeds GitHub search cap" in str(exc)
        else:
            raise AssertionError("expected over-cap day window to fail")


def test_b64_config_overrides_existing_search_config(monkeypatch):
    env_search = {
        "streams": [{
            "name": "pr_velocity",
            "query_template": "org:{org} type:pr is:closed closed:{start}..{end}",
            "mode": "pr_velocity",
        }],
        "scope": {"api_url_base": DEFAULT_API_BASE_URL, "orgs": ["example-env-org"]},
        "backfill": {"start_month": "2026-04"},
    }
    encoded = base64.b64encode(json.dumps(env_search).encode()).decode()
    monkeypatch.setenv("TAP_GITHUB_SEARCH_CONFIG_B64", encoded)

    tap = TapGitHubSearch(config={"search": {"streams": []}})
    streams = tap.discover_streams()

    assert isinstance(streams[0], ConfigurablePrVelocityStream)
    assert streams[0]._search_cfg["search"]["scope"]["orgs"] == ["example-env-org"]
