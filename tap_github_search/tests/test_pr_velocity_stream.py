"""Unit tests for ConfigurablePrVelocityStream and its helpers.

P0 set per the pre-cluster review (see PR #12 description). Covers exception
classification, retry semantics, skip-vs-propagate, day-slicing math, null-node
handling, and mode dispatch -- the code paths the review flagged as risky.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
import requests
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError

from tap_github_search.search_count_streams import (
    ConfigurablePrVelocityStream,
    ConfigurableSearchCountStream,
    NODES_THRESHOLD,
    VALID_API_HOSTS,
    VALID_STREAM_MODES,
    _hours_between,
    _instance_from_api_base,
    _is_transient_graphql_failure,
    create_configurable_streams,
    validate_scope,
    validate_stream_config,
)


# ---------- helpers ----------

class _DummyTap:
    """Minimal tap shim. Mirrors the pattern in test_search_count_streams.py."""

    config: dict = {}
    state: dict = {}
    logger = logging.getLogger("dummy_tap")
    metrics_logger = logging.getLogger("dummy_metrics")
    name = "dummy_tap"
    initialized_at = 0
    rate_limit_buffer = 0

    def setup_mapper(self):
        pass


def _mk_velocity(*, name="pr_velocity", markers=None, reviewer="", instance="github_com"):
    """Build a ConfigurablePrVelocityStream attached to a dummy tap."""
    cfg = {
        "name": name,
        "query_template": "org:{org} type:pr is:closed closed:{start}..{end}",
        "mode": "pr_velocity",
        "markers": markers or [],
        "reviewer_clause": reviewer,
    }
    tap = _DummyTap()
    stream = ConfigurablePrVelocityStream(cfg, tap)
    # Inject the search config the way tap.py does for real runs
    stream._search_cfg = {
        "search": {
            "scope": {
                "api_url_base": "https://api.github.com",
                "orgs": ["test-org"],
                "instance": instance,
            },
            "backfill": {"start_month": "2026-04"},
        }
    }
    return stream


def _fatal_with_response(status_code: int, body_text: str = "", payload: dict | None = None):
    """Build a FatalAPIError carrying a Response-like object.

    Different singer-sdk versions vary on whether the response is auto-attached as
    `.response`; set it explicitly so the test is version-independent.
    """
    resp = Mock()
    resp.status_code = status_code
    resp.text = body_text
    resp.json = Mock(return_value=payload if payload is not None else {})
    exc = FatalAPIError("Graphql error: synthetic")
    exc.response = resp
    return exc


# ---------- P0 tests ----------

class TestIsTransientGraphqlFailure:
    """P0 #1 -- classification correctness for the retry/skip filter."""

    def test_retriable_api_error_is_transient(self):
        assert _is_transient_graphql_failure(RetriableAPIError("503", Mock(status_code=503))) is True

    def test_request_exception_is_transient(self):
        assert _is_transient_graphql_failure(requests.exceptions.ConnectionError("boom")) is True

    def test_fatal_with_internal_error_type_is_transient(self):
        exc = _fatal_with_response(
            200,
            payload={"errors": [{"type": "INTERNAL", "message": "Something went wrong"}]},
        )
        assert _is_transient_graphql_failure(exc) is True

    def test_fatal_with_rate_limited_error_type_is_transient(self):
        exc = _fatal_with_response(
            200,
            payload={"errors": [{"type": "RATE_LIMITED", "message": "API rate limit"}]},
        )
        assert _is_transient_graphql_failure(exc) is True

    def test_fatal_with_forbidden_is_fail_fast(self):
        """SAML / classic-PAT block (e.g. WooCommerce) -- permanent, must NOT retry."""
        exc = _fatal_with_response(
            200,
            payload={"errors": [{"type": "FORBIDDEN", "message": "org forbids access"}]},
        )
        assert _is_transient_graphql_failure(exc) is False

    def test_fatal_with_bad_credentials_is_fail_fast(self):
        exc = _fatal_with_response(401, body_text='{"message":"Bad credentials"}')
        assert _is_transient_graphql_failure(exc) is False

    def test_fatal_with_secondary_rate_limit_403_is_transient(self):
        """The bug the review caught: secondary rate-limit returns 403 with a
        specific message; current code shouldn't conflate it with permanent FORBIDDEN."""
        exc = _fatal_with_response(403, body_text="You have exceeded a secondary rate limit")
        assert _is_transient_graphql_failure(exc) is True

    def test_keyerror_is_not_transient(self):
        """Genuine code bugs must propagate, not get masked as transient."""
        assert _is_transient_graphql_failure(KeyError("missing")) is False


class TestRobustGraphqlRetry:
    """P0 #2-4 -- retry/backoff/exhaustion behavior."""

    def test_retries_transient_then_succeeds(self):
        stream = _mk_velocity()
        calls = []
        good_resp = Mock(name="resp")

        def fake_request(_query, _variables, _api):
            calls.append(1)
            if len(calls) < 3:
                raise RetriableAPIError("502", Mock(status_code=502))
            return good_resp

        with patch.object(stream, "_make_graphql_request", side_effect=fake_request), \
             patch("tap_github_search.search_count_streams.time.sleep") as fake_sleep:
            result = stream._robust_graphql("q", {}, "https://api.github.com", tries=5, max_wall_seconds=1000)
        assert result is good_resp
        assert len(calls) == 3
        assert fake_sleep.call_count == 2  # one sleep before each retry

    def test_fails_fast_on_non_transient(self):
        """4xx / auth errors must not be retried at all."""
        stream = _mk_velocity()
        bad = _fatal_with_response(401, body_text='{"message":"Bad credentials"}')
        with patch.object(stream, "_make_graphql_request", side_effect=bad), \
             patch("tap_github_search.search_count_streams.time.sleep") as fake_sleep:
            with pytest.raises(FatalAPIError):
                stream._robust_graphql("q", {}, "https://api.github.com", tries=5, max_wall_seconds=1000)
        # Should have called exactly once -- no retries on non-transient
        assert fake_sleep.call_count == 0

    def test_exhausts_tries_and_raises_last_exception(self):
        stream = _mk_velocity()
        always_bad = RetriableAPIError("502", Mock(status_code=502))
        with patch.object(stream, "_make_graphql_request", side_effect=always_bad), \
             patch("tap_github_search.search_count_streams.time.sleep"):
            with pytest.raises(RetriableAPIError):
                stream._robust_graphql("q", {}, "https://api.github.com", tries=3, max_wall_seconds=1000)

    def test_aborts_when_wall_time_budget_exceeded(self):
        """Even if tries remain, an outage exceeding max_wall_seconds must give up."""
        stream = _mk_velocity()
        always_bad = RetriableAPIError("502", Mock(status_code=502))
        # patch monotonic so first call returns 0, subsequent calls return a large value
        with patch.object(stream, "_make_graphql_request", side_effect=always_bad), \
             patch("tap_github_search.search_count_streams.time.sleep"), \
             patch("tap_github_search.search_count_streams.time.monotonic",
                   side_effect=[0, 999, 999, 999, 999, 999, 999, 999, 999, 999]):
            with pytest.raises(RetriableAPIError):
                stream._robust_graphql("q", {}, "https://api.github.com", tries=8, max_wall_seconds=10)


class TestEmitWindow:
    """P0 #5-6 -- transient errors skip the window; non-transient propagate."""

    def test_skips_transient_and_increments_counter(self):
        stream = _mk_velocity()
        stream._skipped_windows = 0
        with patch.object(
            stream, "_process_window",
            side_effect=RetriableAPIError("502", Mock(status_code=502)),
        ):
            result = stream._emit_window("q", "https://api.github.com", "github_com", "org", "2026-04", "now", [], "")
        assert result == []
        assert stream._skipped_windows == 1

    def test_propagates_non_transient(self):
        """Genuine bugs (KeyError, auth) must NOT be silently skipped."""
        stream = _mk_velocity()
        stream._skipped_windows = 0
        with patch.object(stream, "_process_window", side_effect=KeyError("not transient")):
            with pytest.raises(KeyError):
                stream._emit_window("q", "https://api.github.com", "github_com", "org", "2026-04", "now", [], "")
        assert stream._skipped_windows == 0  # counter unchanged on non-transient


class TestGetRecordsDispatch:
    """P0 #7-9 -- partition handling and adaptive granularity."""

    def test_skips_zero_count_month(self):
        stream = _mk_velocity()
        with patch.object(stream, "_search_aggregate_count", return_value=0), \
             patch.object(stream, "_emit_window") as fake_emit:
            partition = {
                "org": "test-org",
                "month": "2026-04",
                "search_query": "org:test-org type:pr is:closed closed:2026-04-01..2026-04-30",
                "api_url_base": "https://api.github.com",
            }
            rows = list(stream.get_records(partition))
        assert rows == []
        fake_emit.assert_not_called()  # zero-count months are skipped without fetching

    def test_single_window_when_under_threshold(self):
        """Month with <= NODES_THRESHOLD results is fetched as one paginated pass."""
        stream = _mk_velocity()
        with patch.object(stream, "_search_aggregate_count", return_value=500), \
             patch.object(stream, "_emit_window", return_value=[]) as fake_emit:
            partition = {
                "org": "test-org",
                "month": "2026-04",
                "search_query": "org:test-org type:pr is:closed closed:2026-04-01..2026-04-30",
                "api_url_base": "https://api.github.com",
            }
            list(stream.get_records(partition))
        # Exactly one window fetch, with the original monthly query (no day-slicing)
        assert fake_emit.call_count == 1
        called_query = fake_emit.call_args[0][0]
        assert "closed:2026-04-01..2026-04-30" in called_query

    def test_day_slices_when_over_threshold(self):
        """Month exceeding NODES_THRESHOLD must day-slice; day pre-flight probes day counts."""
        stream = _mk_velocity()
        # First call returns month count (> threshold); subsequent calls are day-count probes (small)
        with patch.object(
            stream, "_search_aggregate_count",
            side_effect=[NODES_THRESHOLD + 500] + [10] * 31,
        ), patch.object(stream, "_emit_window", return_value=[]) as fake_emit:
            partition = {
                "org": "test-org",
                "month": "2026-01",
                "search_query": "org:test-org type:pr is:closed closed:2026-01-01..2026-01-31",
                "api_url_base": "https://api.github.com",
            }
            list(stream.get_records(partition))
        # 31 day-windows for January
        assert fake_emit.call_count == 31
        first_day_query = fake_emit.call_args_list[0][0][0]
        assert "closed:2026-01-01..2026-01-01" in first_day_query

    def test_skips_day_exceeding_preflight_cap(self):
        """P0 #B1 -- if a day's issueCount is at/above DAY_PREFLIGHT_CAP, skip+log
        rather than silently truncate at the 1000-result search cap."""
        stream = _mk_velocity()
        # month-count > threshold (forces day-slicing), then one busy day, then small ones
        day_counts = [NODES_THRESHOLD + 100] + [950] + [5] * 30
        with patch.object(stream, "_search_aggregate_count", side_effect=day_counts), \
             patch.object(stream, "_emit_window", return_value=[]) as fake_emit:
            partition = {
                "org": "test-org",
                "month": "2026-01",
                "search_query": "org:test-org type:pr is:closed closed:2026-01-01..2026-01-31",
                "api_url_base": "https://api.github.com",
            }
            list(stream.get_records(partition))
        # 30 emit calls (busy day skipped), and the counter records the skip
        assert fake_emit.call_count == 30
        assert stream._skipped_windows == 1


class TestNodeToRow:
    """P0 #11 -- null/missing fields must not crash row construction."""

    def test_handles_nulls_gracefully(self):
        stream = _mk_velocity()
        node = {
            "number": 42,
            "repository": {"nameWithOwner": "test-org/test-repo", "name": "test-repo"},
            "createdAt": "2026-04-01T00:00:00Z",
            "closedAt": "2026-04-02T00:00:00Z",
            "mergedAt": None,  # closed-unmerged
            "author": None,    # ghost user
            "mergedBy": None,  # unmerged
            "labels": {"nodes": [None, {"name": "bug"}, None]},  # mixed nulls
            "title": None,
        }
        row = stream._node_to_row(node, "github_com", "test-org", "2026-04", "now")
        assert row["pr_number"] == 42
        assert row["author_login"] is None
        assert row["merged_by_login"] is None
        assert row["label_names"] == '["bug"]'   # JSON-encoded; nulls filtered, valid kept
        assert row["outcome"] == "closed_unmerged"  # mergedAt None -> unmerged
        assert row["hours_to_close"] == 24.0
        assert row["is_ai_authored"] is False
        assert row["is_ai_reviewed"] is False


# ---------- Bonus / supporting tests (cheap, prevent regressions) ----------

class TestHoursBetween:
    def test_handles_none(self):
        assert _hours_between(None, "2026-04-01T00:00:00Z") is None
        assert _hours_between("2026-04-01T00:00:00Z", None) is None
        assert _hours_between(None, None) is None

    def test_iso_with_z_suffix(self):
        result = _hours_between("2026-04-01T00:00:00Z", "2026-04-01T12:00:00Z")
        assert result == 12.0


class TestInstanceFromApiBase:
    def test_known_hosts(self):
        assert _instance_from_api_base("https://api.github.com") == "github_com"
        assert _instance_from_api_base("https://github.a8c.com/api/v3") == "a8c_ghe"
        assert _instance_from_api_base("https://github.tumblr.net/api/v3") == "tumblr_ghe"

    def test_unknown_falls_back(self):
        assert _instance_from_api_base("https://example.invalid") == "unknown"
        assert _instance_from_api_base("") == "unknown"


class TestCreateConfigurableStreamsModeDispatch:
    def test_dispatches_pr_velocity_mode(self):
        tap = _DummyTap()
        config = {
            "search": {
                "streams": [{
                    "name": "vel",
                    "query_template": "org:{org} type:pr is:closed closed:{start}..{end}",
                    "mode": "pr_velocity",
                }],
                "scope": {"api_url_base": "https://api.github.com", "orgs": ["x"]},
                "backfill": {"start_month": "2026-04"},
            }
        }
        streams = create_configurable_streams(tap, config_override=config)
        assert len(streams) == 1
        assert isinstance(streams[0], ConfigurablePrVelocityStream)

    def test_count_stream_when_mode_absent(self):
        tap = _DummyTap()
        config = {
            "search": {
                "streams": [{
                    "name": "cnt",
                    "query_template": "org:{org} is:pr is:merged merged:{start}..{end}",
                }],
                "scope": {"api_url_base": "https://api.github.com", "orgs": ["x"]},
                "backfill": {"start_month": "2026-04"},
            }
        }
        streams = create_configurable_streams(tap, config_override=config)
        assert len(streams) == 1
        # Should be the count stream class, NOT velocity
        assert isinstance(streams[0], ConfigurableSearchCountStream)
        assert not isinstance(streams[0], ConfigurablePrVelocityStream)


class TestValidateStreamConfig:
    """H5 -- mode whitelist guards against silent typo fallback."""

    def test_accepts_known_modes(self):
        for mode in (None, "pr_velocity"):
            errs = validate_stream_config({
                "name": "x", "query_template": "{org} {start} {end}", "mode": mode,
            })
            assert not any("Unknown mode" in e for e in errs)

    def test_rejects_typo_in_mode(self):
        errs = validate_stream_config({
            "name": "x", "query_template": "{org} {start} {end}", "mode": "pr-velocity",  # hyphen typo
        })
        assert any("Unknown mode" in e for e in errs)


class TestValidateScope:
    """SSRF allowlist on api_url_base. Defense-in-depth against scope misconfig."""

    def test_accepts_known_hosts(self):
        for host in VALID_API_HOSTS:
            errs = validate_scope({"api_url_base": f"https://{host}/api/v3"})
            assert errs == [], f"unexpected errors for known host {host}: {errs}"

    def test_rejects_unknown_host(self):
        errs = validate_scope({"api_url_base": "https://evil.example.com/api"})
        assert any("not in allowlist" in e for e in errs)

    def test_rejects_http_scheme(self):
        errs = validate_scope({"api_url_base": "http://api.github.com"})
        assert any("must use https" in e for e in errs)

    def test_allows_missing_api_url_base(self):
        # api_url_base is optional; partitions() falls back to https://api.github.com
        assert validate_scope({}) == []

    def test_create_configurable_streams_raises_on_bad_scope(self):
        tap = _DummyTap()
        config = {
            "search": {
                "streams": [{
                    "name": "vel",
                    "query_template": "org:{org} type:pr is:closed closed:{start}..{end}",
                    "mode": "pr_velocity",
                }],
                "scope": {"api_url_base": "https://evil.example.com", "orgs": ["x"]},
                "backfill": {"start_month": "2026-04"},
            }
        }
        with pytest.raises(ValueError, match="not in allowlist"):
            create_configurable_streams(tap, config_override=config)

    def test_authenticator_rejects_non_allowlisted_host(self):
        """Two-layer SSRF defense: the authenticator must refuse to emit a PAT
        to a host outside VALID_API_HOSTS even if a stream is constructed
        outside the create_configurable_streams factory."""
        from tap_github_search.authenticator import WrapperGitHubTokenAuthenticator
        stream = SimpleNamespace(
            _search_cfg={"search": {"scope": {"api_url_base": "https://evil.example.com"}}},
            config={},
        )
        with pytest.raises(ValueError, match="Refusing to authenticate"):
            WrapperGitHubTokenAuthenticator._extract_api_base_url_from_stream(stream)

    def test_authenticator_accepts_allowlisted_host(self):
        from tap_github_search.authenticator import WrapperGitHubTokenAuthenticator
        for host in VALID_API_HOSTS:
            stream = SimpleNamespace(
                _search_cfg={"search": {"scope": {"api_url_base": f"https://{host}/api/v3"}}},
                config={},
            )
            url = WrapperGitHubTokenAuthenticator._extract_api_base_url_from_stream(stream)
            assert host in url
