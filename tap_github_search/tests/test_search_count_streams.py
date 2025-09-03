from __future__ import annotations

from datetime import date
import os
import types
import logging
from unittest.mock import Mock, patch
import pytest
import requests

from tap_github_search.search_count_streams import (
    ConfigurableSearchCountStream,
    create_configurable_streams,
    validate_stream_config,
    SearchCountStreamBase,
    BATCH_SIZE,
)
from tap_github_search.tap import TapGitHubSearch
from tap_github_search.authenticator import GHEPersonalTokenManager
from tap_github_search.utils.date_utils import (
    month_range,
    month_to_date,
    get_last_complete_month,
    get_last_complete_month_date,
)


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


# Authentication Tests
@patch('requests.get')
def test_ghe_token_validation_success(mock_get):
    mock_get.return_value.status_code = 200
    mock_get.return_value.raise_for_status.return_value = None
    
    manager = GHEPersonalTokenManager("token123", "https://github.enterprise.com/api/v3")
    assert manager.is_valid_token() == True
    
    mock_get.assert_called_once_with(
        url="https://github.enterprise.com/api/v3/rate_limit",
        headers={"Authorization": "token token123"}
    )


@patch('requests.get')
def test_ghe_token_validation_failure(mock_get):
    mock_get.return_value.status_code = 401
    mock_get.return_value.reason = "Unauthorized"
    mock_get.return_value.content = b"Bad credentials"
    mock_get.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError()
    
    manager = GHEPersonalTokenManager("invalid_token", "https://github.enterprise.com/api/v3")
    assert manager.is_valid_token() == False


@patch('requests.get')
def test_ghe_token_validation_connection_error(mock_get):
    mock_get.side_effect = requests.exceptions.ConnectionError()
    
    manager = GHEPersonalTokenManager("token123", "https://github.enterprise.com/api/v3")
    assert manager.is_valid_token() == False


# Date Utilities Tests
def test_month_range():
    months = month_range("2024-11", "2025-01")
    expected = ["2024-11", "2024-12", "2025-01"]
    assert months == expected


def test_month_range_single_month():
    months = month_range("2024-06", "2024-06")
    expected = ["2024-06"]
    assert months == expected


def test_month_to_date():
    result = month_to_date("2024-03")
    expected = date(2024, 3, 1)
    assert result == expected


def test_get_last_complete_month():
    with patch('tap_github_search.utils.date_utils.date') as mock_date:
        mock_date.today.return_value = date(2024, 3, 15)
        result = get_last_complete_month()
        assert result == "2024-02"


def test_get_last_complete_month_january():
    with patch('tap_github_search.utils.date_utils.date') as mock_date:
        mock_date.today.return_value = date(2024, 1, 15)
        result = get_last_complete_month()
        assert result == "2023-12"


def test_get_last_complete_month_date():
    with patch('tap_github_search.utils.date_utils.date') as mock_date:
        mock_date.today.return_value = date(2024, 3, 15)
        mock_date.side_effect = lambda *args, **kwargs: date(*args, **kwargs)
        result = get_last_complete_month_date()
        assert result == date(2024, 2, 1)


# Error Handling Tests
def test_validate_stream_config_missing_placeholders():
    config = {
        "name": "test",
        "query_template": "org:{org} type:issue"  # missing {start} and {end}
    }
    errors = validate_stream_config(config)
    assert len(errors) >= 2
    assert any("{start}" in error for error in errors)
    assert any("{end}" in error for error in errors)


def test_validate_stream_config_invalid_name():
    config = {
        "name": "test name with spaces!",
        "query_template": "org:{org} type:issue created:{start}..{end}"
    }
    errors = validate_stream_config(config)
    assert any("alphanumeric" in error for error in errors)


def test_validate_stream_config_empty_fields():
    config = {
        "name": "",
        "query_template": ""
    }
    errors = validate_stream_config(config)
    assert len(errors) >= 2
    assert any("name" in error for error in errors)
    assert any("query_template" in error for error in errors)


# Partition Generation Tests
def test_configurable_stream_partitions_with_orgs():
    stream_config = {
        "name": "test",
        "query_template": "org:{org} type:issue created:{start}..{end}"
    }
    mock_tap = Mock()
    mock_tap.config = {}
    mock_tap.state = {}
    
    search_config = {
        "search": {
            "scope": {
                "orgs": ["TestOrg"],
                "api_url_base": "https://api.github.com"
            },
            "backfill": {
                "start_month": "2024-01",
                "end_month": "2024-01"
            }
        }
    }
    
    stream = ConfigurableSearchCountStream(stream_config, mock_tap)
    stream._search_cfg = search_config
    
    partitions = stream.partitions
    assert len(partitions) == 1
    assert partitions[0]["org"] == "TestOrg"
    assert partitions[0]["month"] == "2024-01"
    assert "org:TestOrg" in partitions[0]["search_query"]


def test_configurable_stream_partitions_with_repos():
    stream_config = {
        "name": "test", 
        "query_template": "repo:{org} type:issue created:{start}..{end}"
    }
    mock_tap = Mock()
    mock_tap.config = {}
    mock_tap.state = {}
    
    search_config = {
        "search": {
            "scope": {
                "repos": ["TestOrg/test-repo"],
                "api_url_base": "https://api.github.com"
            },
            "backfill": {
                "start_month": "2024-01", 
                "end_month": "2024-01"
            }
        }
    }
    
    stream = ConfigurableSearchCountStream(stream_config, mock_tap)
    stream._search_cfg = search_config
    
    partitions = stream.partitions
    assert len(partitions) == 1
    assert partitions[0]["org"] == "TestOrg"
    assert "repo:TestOrg/test-repo" in partitions[0]["search_query"]


def test_create_streams_invalid_config():
    config = {
        "search": {
            "streams": [
                {
                    "name": "invalid",
                    "query_template": "org:{org} type:issue"  # missing placeholders
                }
            ]
        }
    }
    mock_tap = Mock()
    mock_tap.logger = Mock()
    
    streams = create_configurable_streams(mock_tap, config)
    assert len(streams) == 0
    mock_tap.logger.warning.assert_called()


# --- Open-in-month fan-out tests (unit, no network) ---

class _DummyTap:
    config = {}
    state = {}
    logger = logging.getLogger("dummy_tap")
    metrics_logger = logging.getLogger("dummy_metrics")
    name = "dummy_tap"
    initialized_at = 0


@pytest.fixture(autouse=True)
def big_slice_days(monkeypatch):
    # Make weekly slicer effectively "no-op" in tests (one slice per RANGE).
    monkeypatch.setenv("GITHUB_SEARCH_SLICE_DAYS", "100000")


def _mk_stream():
    # Minimal concrete instance; base class methods are what we test.
    return SearchCountStreamBase(tap=_DummyTap(), name="test", schema=None, path=None)


def test_range_passthrough(monkeypatch):
    """
    For queries already containing created:START..END, we expect direct RANGE handling
    (no fan-out) and a single call to _get_repo_counts_from_nodes (thanks to big slice days).
    """
    s = _mk_stream()
    calls = {"n": 0}

    def fake_nodes(query, api_url_base):
        calls["n"] += 1
        assert "created:2025-01-01..2025-01-31" in query
        return {"r1": 10}

    monkeypatch.setattr(s, "_get_repo_counts_from_nodes", fake_nodes)
    out = s._search_with_auto_slicing("org:X is:issue created:2025-01-01..2025-01-31", "https://api.github.com")

    assert calls["n"] == 1
    assert out == {"r1": 10}


def test_no_created_falls_back(monkeypatch):
    """
    If there's no created: qualifier, method should fall back directly to _get_repo_counts_from_nodes.
    """
    s = _mk_stream()
    calls = {"n": 0}

    def fake_nodes(query, api_url_base):
        calls["n"] += 1
        assert "created:" not in query
        return {"rZ": 7}

    monkeypatch.setattr(s, "_get_repo_counts_from_nodes", fake_nodes)
    out = s._search_with_auto_slicing("org:X is:issue label:foo", "https://api.github.com")

    assert calls["n"] == 1
    assert out == {"rZ": 7}


def test_repo_breakdown_batches_issuecount(monkeypatch):
    """
    Test simplified batching implementation for org-scoped breakdown.
    """
    s = _mk_stream()

    # Provide a fixed repo list  
    monkeypatch.setattr(s, "_list_repos_for_org", lambda api, org: [
        "r1", "r2", "r3", "r4", "r5"
    ])

    counts_by_repo = {"r1": 5, "r2": 0, "r3": 3, "r4": 7, "r5": 1}

    def fake_batch(queries, api):
        out = []
        for q in queries:
            # q is like: repo:Automattic/rX rest...
            try:
                repo_part = [p for p in q.split() if p.startswith("repo:")][0]
                name = repo_part.split("/")[-1]
            except Exception:
                name = ""
            out.append(counts_by_repo.get(name, 0))
        return out

    monkeypatch.setattr(s, "_search_aggregate_count_batch", fake_batch)
    
    # Mock total count to trigger repo listing path
    monkeypatch.setattr(s, "_search_aggregate_count", lambda q, api: 2000)

    q = "org:Automattic is:issue created:2025-01-01..2025-01-31"
    out = s._search_with_repo_breakdown(q, "https://api.github.com")

    # Zero counts are filtered out by simplified implementation
    assert out == {"r1": 5, "r3": 3, "r4": 7, "r5": 1}


def test_repo_scoped_fast_path_issuecount(monkeypatch):
    """
    Repo-scoped query should issue a single aggregate count and return that mapping.
    """
    s = _mk_stream()

    def fake_single(query, api):
        assert query.startswith("repo:Automattic/calypso ")
        return 42

    monkeypatch.setattr(s, "_search_aggregate_count", fake_single)

    q = "repo:Automattic/calypso is:issue created:2025-02-01..2025-02-28"
    out = s._search_with_repo_breakdown(q, "https://api.github.com")
    assert out == {"calypso": 42}


def test_graphql_variables_query_template():
    """
    Test that the essential GraphQL variables fix uses the correct template.
    """
    s = _mk_stream()
    
    # Verify GRAPHQL_SEARCH_COUNT_ONLY uses variables
    assert "query SearchCount($q: String!)" in s.GRAPHQL_SEARCH_COUNT_ONLY
    assert "search(query: $q" in s.GRAPHQL_SEARCH_COUNT_ONLY
    assert "issueCount" in s.GRAPHQL_SEARCH_COUNT_ONLY
