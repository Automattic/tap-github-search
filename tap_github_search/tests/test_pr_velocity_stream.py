from __future__ import annotations

import base64
from datetime import date, datetime
import json
import logging
from unittest.mock import patch

import pytest

from tap_github_search.pr_velocity_stream import ConfigurablePrVelocityStream
from tap_github_search.search_count_streams import (
    NODES_THRESHOLD,
    create_configurable_streams,
)
from tap_github_search.tap import TapGitHubSearch


DEFAULT_API_BASE_URL = "https://api.github.com"


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


class _GraphqlResponse:
    def __init__(self, *, nodes, has_next=False, end_cursor=None):
        self.payload = {
            "data": {
                "search": {
                    "nodes": nodes,
                    "pageInfo": {"hasNextPage": has_next, "endCursor": end_cursor},
                }
            }
        }

    def json(self):
        return self.payload


class _RepoResponse:
    def __init__(self, *, nodes, has_next=False, end_cursor=None):
        self.payload = {
            "data": {
                "organization": {
                    "repositories": {
                        "nodes": nodes,
                        "pageInfo": {"hasNextPage": has_next, "endCursor": end_cursor},
                    }
                }
            }
        }

    def json(self):
        return self.payload


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


def _velocity_context(search_query):
    return {
        "org": "example-org",
        "month": "2026-04",
        "search_query": search_query,
        "api_url_base": DEFAULT_API_BASE_URL,
    }


def _processed_queries(fake_process):
    return [call.args[0] for call in fake_process.call_args_list]


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


def test_iter_pr_nodes_uses_end_cursor_for_next_page():
    stream = _mk_velocity()
    first_node = {
        "number": 1,
        "repository": {"nameWithOwner": "example-org/example-repo"},
    }
    second_node = {
        "number": 2,
        "repository": {"nameWithOwner": "example-org/example-repo"},
    }

    with patch.object(
        stream,
        "_make_graphql_request",
        side_effect=[
            _GraphqlResponse(
                nodes=[first_node], has_next=True, end_cursor="cursor-one"
            ),
            _GraphqlResponse(nodes=[second_node], has_next=False, end_cursor=None),
        ],
    ) as fake_request:
        rows = list(
            stream._iter_pr_nodes(
                "org:example-org type:pr is:closed closed:2026-04-01..2026-04-30",
                DEFAULT_API_BASE_URL,
            )
        )

    assert rows == [first_node, second_node]
    assert fake_request.call_args_list[0][0][1]["after"] is None
    assert fake_request.call_args_list[1][0][1]["after"] == "cursor-one"


def test_collect_pr_ids_uses_end_cursor_for_next_page():
    stream = _mk_velocity()
    first_node = {
        "number": 1,
        "repository": {"nameWithOwner": "example-org/example-repo"},
    }
    second_node = {
        "number": 2,
        "repository": {"nameWithOwner": "example-org/example-repo"},
    }

    with patch.object(
        stream,
        "_make_graphql_request",
        side_effect=[
            _GraphqlResponse(
                nodes=[first_node], has_next=True, end_cursor="cursor-one"
            ),
            _GraphqlResponse(nodes=[second_node], has_next=False, end_cursor=None),
        ],
    ) as fake_request:
        ids = stream._collect_pr_ids(
            "org:example-org type:pr is:closed reviewed-by:review-bot",
            DEFAULT_API_BASE_URL,
        )

    assert ids == {"example-org/example-repo#1", "example-org/example-repo#2"}
    assert fake_request.call_args_list[0][0][1]["after"] is None
    assert fake_request.call_args_list[1][0][1]["after"] == "cursor-one"


def test_list_all_repos_for_org_uses_end_cursor_for_next_page():
    stream = _mk_velocity()
    with patch.object(
        stream,
        "_make_graphql_request",
        side_effect=[
            _RepoResponse(
                nodes=[{"name": "repo-one"}], has_next=True, end_cursor="cursor-one"
            ),
            _RepoResponse(
                nodes=[{"name": "repo-two"}],
                has_next=False,
                end_cursor=None,
            ),
        ],
    ) as fake_request:
        repos = stream._list_all_repos_for_org(DEFAULT_API_BASE_URL, "example-org")

    assert repos == ["repo-one", "repo-two"]
    assert fake_request.call_args_list[0][0][1]["after"] is None
    assert fake_request.call_args_list[1][0][1]["after"] == "cursor-one"


def test_get_records_emits_json_serializable_synced_at():
    stream = _mk_velocity()
    node = {
        "number": 1,
        "repository": {"nameWithOwner": "example-org/example-repo", "name": "example-repo"},
        "createdAt": "2026-04-01T00:00:00Z",
        "closedAt": "2026-04-01T01:00:00Z",
        "mergedAt": "2026-04-01T01:00:00Z",
        "author": {"login": "author-one"},
        "bodyText": "",
    }

    with patch.object(stream, "_search_aggregate_count", return_value=1), \
         patch.object(stream, "_iter_pr_nodes", return_value=iter([node])):
        row = next(stream.get_records(_velocity_context(
            "org:example-org type:pr is:closed closed:2026-04-01..2026-04-30"
        )))

    assert isinstance(row["synced_at"], str)
    assert row["synced_at"].endswith("Z")
    assert datetime.fromisoformat(row["synced_at"].replace("Z", "+00:00")).tzinfo is not None
    json.dumps(row)


def test_get_records_day_slices_when_month_exceeds_search_cap():
    stream = _mk_velocity()
    with patch.object(stream, "_search_aggregate_count", side_effect=[NODES_THRESHOLD + 1] + [5] * 30), \
         patch.object(stream, "_process_window", return_value=[]) as fake_process:
        list(stream.get_records(_velocity_context(
            "org:example-org type:pr is:closed closed:2026-04-01..2026-04-30"
        )))

    assert fake_process.call_count == 30
    assert "closed:2026-04-01..2026-04-01" in _processed_queries(fake_process)[0]


def test_get_records_repo_slices_when_single_org_day_exceeds_search_cap():
    stream = _mk_velocity()

    def count(query, _api_url_base):
        if "closed:2026-04-01..2026-04-30" in query:
            return NODES_THRESHOLD + 1
        if "closed:2026-04-01..2026-04-01" in query:
            return NODES_THRESHOLD + 1
        return 0

    with patch.object(stream, "_search_aggregate_count", side_effect=count), \
         patch.object(
             stream,
             "_list_all_repos_for_org",
             return_value=["repo-one", "repo-two"],
         ), \
         patch.object(
             stream,
             "_get_repo_counts_via_batching",
             return_value={"repo-one": 800, "repo-two": 201},
         ) as fake_repo_counts, \
         patch.object(stream, "_process_window", return_value=[]) as fake_process:
        list(stream.get_records(_velocity_context(
            "org:example-org type:pr is:closed closed:2026-04-01..2026-04-30"
        )))

    fake_repo_counts.assert_called_once_with(
        ["repo-one", "repo-two"],
        "example-org",
        "type:pr is:closed closed:2026-04-01..2026-04-01",
        DEFAULT_API_BASE_URL,
    )
    assert _processed_queries(fake_process) == [
        "repo:example-org/repo-one type:pr is:closed closed:2026-04-01..2026-04-01",
        "repo:example-org/repo-two type:pr is:closed closed:2026-04-01..2026-04-01",
    ]


def test_get_records_fails_when_repo_counts_do_not_cover_capped_day():
    stream = _mk_velocity()

    def count(query, _api_url_base):
        if "closed:2026-04-01..2026-04-30" in query:
            return NODES_THRESHOLD + 1
        if "closed:2026-04-01..2026-04-01" in query:
            return NODES_THRESHOLD + 1
        return 0

    with patch.object(stream, "_search_aggregate_count", side_effect=count), \
         patch.object(stream, "_list_all_repos_for_org", return_value=["repo-one"]), \
         patch.object(stream, "_get_repo_counts_via_batching", return_value={"repo-one": 1}), \
         pytest.raises(RuntimeError, match="repo split did not preserve GitHub search count"):
        list(stream.get_records(_velocity_context(
            "org:example-org type:pr is:closed closed:2026-04-01..2026-04-30"
        )))


def test_get_records_created_slices_when_org_repo_day_exceeds_search_cap():
    stream = _mk_velocity()

    def count(query, _api_url_base):
        if "created:2026-03-31..2026-04-01" in query:
            return NODES_THRESHOLD + 1
        if "created:2026-03-31..2026-03-31" in query:
            return 700
        if "created:2026-04-01..2026-04-01" in query:
            return 500
        if "closed:2026-04-01..2026-04-30" in query:
            return NODES_THRESHOLD + 1
        if "closed:2026-04-01..2026-04-01" in query:
            return NODES_THRESHOLD + 1
        return 0

    with patch("tap_github_search.pr_velocity_stream.CREATED_SEARCH_START_DATE", date(2026, 3, 31)), \
         patch.object(stream, "_search_aggregate_count", side_effect=count), \
         patch.object(
             stream,
             "_list_all_repos_for_org",
             return_value=["big-repo"],
         ), \
         patch.object(
             stream,
             "_get_repo_counts_via_batching",
             return_value={"big-repo": NODES_THRESHOLD + 1},
         ), \
         patch.object(stream, "_process_window", return_value=[]) as fake_process:
        list(stream.get_records(_velocity_context(
            "org:example-org type:pr is:closed closed:2026-04-01..2026-04-30"
        )))

    assert _processed_queries(fake_process) == [
        "repo:example-org/big-repo type:pr is:closed "
        "closed:2026-04-01..2026-04-01 created:2026-03-31..2026-03-31",
        "repo:example-org/big-repo type:pr is:closed "
        "closed:2026-04-01..2026-04-01 created:2026-04-01..2026-04-01",
    ]


def test_get_records_created_slices_when_repo_scoped_day_exceeds_search_cap():
    stream = _mk_velocity()

    def count(query, _api_url_base):
        if "created:2026-03-31..2026-04-01" in query:
            return NODES_THRESHOLD + 1
        if "created:2026-03-31..2026-03-31" in query:
            return 700
        if "created:2026-04-01..2026-04-01" in query:
            return 500
        return NODES_THRESHOLD + 1

    with patch("tap_github_search.pr_velocity_stream.CREATED_SEARCH_START_DATE", date(2026, 3, 31)), \
         patch.object(stream, "_search_aggregate_count", side_effect=count), \
         patch.object(stream, "_process_window", return_value=[]) as fake_process:
        list(stream.get_records(_velocity_context(
            (
                "repo:example-org/big-repo type:pr is:closed "
                "closed:2026-04-01..2026-04-01"
            )
        )))

    assert _processed_queries(fake_process) == [
        "repo:example-org/big-repo type:pr is:closed "
        "closed:2026-04-01..2026-04-01 created:2026-03-31..2026-03-31",
        "repo:example-org/big-repo type:pr is:closed "
        "closed:2026-04-01..2026-04-01 created:2026-04-01..2026-04-01",
    ]


def test_iter_created_range_queries_recurses_until_leaf_counts_fit_cap():
    stream = _mk_velocity()
    base_query = (
        "repo:example-org/big-repo type:pr is:closed "
        "closed:2026-04-04..2026-04-04"
    )

    def count(query, _api_url_base):
        if "created:2026-04-01..2026-04-02" in query:
            return NODES_THRESHOLD + 1
        if "created:2026-04-01..2026-04-01" in query:
            return 600
        if "created:2026-04-02..2026-04-02" in query:
            return 500
        return 0

    with patch.object(stream, "_search_aggregate_count", side_effect=count):
        queries = list(stream._iter_created_range_queries(
            base_query,
            DEFAULT_API_BASE_URL,
            "example-org/big-repo",
            date(2026, 4, 1),
            date(2026, 4, 4),
            NODES_THRESHOLD + 1,
        ))

    assert queries == [
        "repo:example-org/big-repo type:pr is:closed "
        "closed:2026-04-04..2026-04-04 created:2026-04-01..2026-04-01",
        "repo:example-org/big-repo type:pr is:closed "
        "closed:2026-04-04..2026-04-04 created:2026-04-02..2026-04-02",
    ]


def test_get_records_fails_when_query_already_has_created_qualifier():
    stream = _mk_velocity()

    with patch.object(stream, "_search_aggregate_count", return_value=NODES_THRESHOLD + 1), \
         pytest.raises(RuntimeError, match="already scoped by created date"):
        list(stream.get_records(_velocity_context(
            "repo:example-org/big-repo type:pr is:closed "
            "closed:2026-04-01..2026-04-01 created:>=2026-03-01"
        )))


def test_get_records_fails_when_created_day_exceeds_search_cap():
    stream = _mk_velocity()

    with patch("tap_github_search.pr_velocity_stream.CREATED_SEARCH_START_DATE", date(2026, 4, 1)), \
         patch.object(stream, "_search_aggregate_count", return_value=NODES_THRESHOLD + 1), \
         pytest.raises(RuntimeError, match="repo created-day window exceeds GitHub search cap"):
        list(stream.get_records(_velocity_context(
            "repo:example-org/big-repo type:pr is:closed "
            "closed:2026-04-01..2026-04-01"
        )))


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
