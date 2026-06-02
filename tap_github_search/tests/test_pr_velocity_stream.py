from __future__ import annotations

import base64
import json
import logging
from unittest.mock import Mock, patch

import pytest
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError

from tap_github_search.github_hosts import DEFAULT_API_BASE_URL
from tap_github_search.search_count_streams import (
    ConfigurablePrVelocityStream,
    NODES_THRESHOLD,
    _is_transient_graphql_failure,
    create_configurable_streams,
    validate_scope,
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
                "orgs": ["test-org"],
                "instance": instance,
            },
            "backfill": {"start_month": "2026-04"},
        }
    }
    return stream


def _fatal_with_response(status_code: int, body_text: str = "", payload: dict | None = None):
    response = Mock()
    response.status_code = status_code
    response.text = body_text
    response.json = Mock(return_value=payload if payload is not None else {})
    exc = FatalAPIError("GraphQL error: synthetic")
    exc.response = response
    return exc


def test_validate_scope_is_generic_not_deployment_allowlist():
    assert validate_scope({"api_url_base": "https://github.example.invalid/api/v3", "instance": "example_ghe"}) == []
    assert any("must use https" in err for err in validate_scope({"api_url_base": "http://github.example.invalid"}))
    assert any("must not include credentials" in err for err in validate_scope({"api_url_base": "https://u:p@github.example.invalid"}))
    assert any("must not include query" in err for err in validate_scope({"api_url_base": "https://github.example.invalid/api/v3?x=1"}))


def test_non_default_api_base_requires_instance():
    errors = validate_scope({"api_url_base": "https://github.example.invalid/api/v3"})
    assert any("scope.instance is required" in err for err in errors)


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
            "scope": {"api_url_base": DEFAULT_API_BASE_URL, "orgs": ["test-org"]},
            "backfill": {"start_month": "2026-04"},
        }
    }
    streams = create_configurable_streams(tap, config_override=config)
    assert len(streams) == 1
    assert type(streams[0]) is ConfigurablePrVelocityStream


def test_transient_error_classification():
    assert _is_transient_graphql_failure(RetriableAPIError("502", Mock(status_code=502))) is True
    assert _is_transient_graphql_failure(_fatal_with_response(
        200,
        payload={"errors": [{"type": "INTERNAL", "message": "backend timeout"}]},
    )) is True
    assert _is_transient_graphql_failure(_fatal_with_response(401, body_text="Bad credentials")) is False
    assert _is_transient_graphql_failure(KeyError("bug")) is False


def test_robust_graphql_retries_transient_then_succeeds():
    stream = _mk_velocity()
    calls = []
    good_response = Mock(name="response")

    def fake_request(_query, _variables, _api):
        calls.append(1)
        if len(calls) < 3:
            raise RetriableAPIError("502", Mock(status_code=502))
        return good_response

    with patch.object(stream, "_make_graphql_request", side_effect=fake_request), \
         patch("tap_github_search.search_count_streams.time.sleep") as fake_sleep:
        result = stream._robust_graphql("q", {}, DEFAULT_API_BASE_URL, tries=5, max_wall_seconds=1000)

    assert result is good_response
    assert len(calls) == 3
    assert fake_sleep.call_count == 2


def test_process_window_sets_minimal_fields_and_ai_flags():
    stream = _mk_velocity(
        markers=['"AI marker"', "assistant-marker"],
        reviewer="reviewed-by:review-bot",
    )
    nodes = [
        {
            "number": 1,
            "repository": {"nameWithOwner": "test-org/repo", "name": "repo"},
            "createdAt": "2026-04-01T00:00:00Z",
            "closedAt": "2026-04-01T01:00:00Z",
            "mergedAt": "2026-04-01T01:00:00Z",
            "author": {"login": "author-one"},
            "bodyText": "Contains the AI marker.",
        },
        {
            "number": 2,
            "repository": {"nameWithOwner": "test-org/repo", "name": "repo"},
            "createdAt": "2026-04-01T00:00:00Z",
            "closedAt": "2026-04-01T02:00:00Z",
            "mergedAt": None,
            "author": None,
            "bodyText": "Hand-authored PR.",
        },
    ]

    with patch.object(stream, "_iter_pr_nodes", return_value=iter(nodes)), \
         patch.object(stream, "_collect_pr_ids", return_value={"test-org/repo#2"}):
        rows = list(stream._process_window(
            "org:test-org type:pr is:closed closed:2026-04-01..2026-04-01",
            DEFAULT_API_BASE_URL,
            "github_com",
            "test-org",
            "2026-04",
            "now",
            stream.stream_config["markers"],
            stream.stream_config["reviewer_clause"],
        ))

    assert set(rows[0]) == {
        "instance", "organization", "repo", "pr_number", "author_login", "created_at",
        "closed_at", "merged_at", "hours_to_close", "outcome",
        "is_ai_authored", "is_ai_reviewed", "month", "synced_at",
    }
    assert rows[0]["organization"] == "test-org"
    assert rows[0]["hours_to_close"] == 1.0
    assert rows[0]["is_ai_authored"] is True
    assert rows[0]["is_ai_reviewed"] is False
    assert rows[1]["outcome"] == "closed_unmerged"
    assert rows[1]["author_login"] is None
    assert rows[1]["is_ai_reviewed"] is True


def test_get_records_day_slices_when_month_exceeds_search_cap():
    stream = _mk_velocity()
    with patch.object(stream, "_search_aggregate_count", side_effect=[NODES_THRESHOLD + 1] + [5] * 30), \
         patch.object(stream, "_emit_window", return_value=[]) as fake_emit:
        list(stream.get_records({
            "org": "test-org",
            "month": "2026-04",
            "search_query": "org:test-org type:pr is:closed closed:2026-04-01..2026-04-30",
            "api_url_base": DEFAULT_API_BASE_URL,
        }))

    assert fake_emit.call_count == 30
    assert "closed:2026-04-01..2026-04-01" in fake_emit.call_args_list[0][0][0]


def test_b64_config_overrides_existing_search_config(monkeypatch):
    env_search = {
        "streams": [{
            "name": "pr_velocity",
            "query_template": "org:{org} type:pr is:closed closed:{start}..{end}",
            "mode": "pr_velocity",
        }],
        "scope": {"api_url_base": DEFAULT_API_BASE_URL, "orgs": ["env-org"]},
        "backfill": {"start_month": "2026-04"},
    }
    encoded = base64.b64encode(json.dumps(env_search).encode()).decode()
    monkeypatch.setenv("TAP_GITHUB_SEARCH_CONFIG_B64", encoded)

    tap = TapGitHubSearch(config={"search": {"streams": []}})
    streams = tap.discover_streams()

    assert isinstance(streams[0], ConfigurablePrVelocityStream)
    assert streams[0]._search_cfg["search"]["scope"]["orgs"] == ["env-org"]
