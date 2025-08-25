"""GitHub search count streams - simplified Singer SDK implementation (wrapper)."""

from __future__ import annotations

import calendar
import re
from datetime import date, datetime, timedelta
from typing import Any, ClassVar, Iterable, Mapping
import os
from collections import Counter

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_github.client import GitHubGraphqlStream
from tap_github_search.authenticator import WrapperGitHubTokenAuthenticator

from tap_github_search.utils.date_utils import (
    get_last_complete_month,
    get_last_complete_month_date,
    month_to_date,
    month_range,
)


class SearchCountStreamBase(GitHubGraphqlStream):
    """Base class for configurable GitHub search count streams."""

    stream_type: ClassVar[str] = "custom"

    PRIMARY_KEYS: ClassVar[list[str]] = ["search", "month", "org", "repo"]
    REPLICATION_METHOD: ClassVar[str] = "INCREMENTAL"
    REPLICATION_KEY: ClassVar[str] = "month"
    STATE_KEYS: ClassVar[list[str]] = ["org", "repo"]

    primary_keys: ClassVar[list[str]] = PRIMARY_KEYS
    replication_method: ClassVar[str] = REPLICATION_METHOD
    replication_key: ClassVar[str] = REPLICATION_KEY
    state_partitioning_keys: ClassVar[list[str]] = STATE_KEYS

    def __init__(self, tap, name=None, schema=None, path=None):
        self.tap = tap
        super().__init__(tap=tap, name=name or self.name, schema=schema or self.get_schema(), path=path)

    _authenticator: GitHubTokenAuthenticator | None = None

    @classmethod
    def _build_search_schema(cls) -> dict:
        return th.PropertiesList(
            th.Property("search", th.StringType, required=True),
            th.Property("query", th.StringType, required=True),
            th.Property("org", th.StringType, required=True),
            th.Property("repo", th.StringType, required=True),
            th.Property("month", th.StringType, required=True),
            th.Property("count", th.IntegerType, required=True),
            th.Property("updated_at", th.DateTimeType),
        ).to_dict()

    @classmethod
    def get_schema(cls) -> dict:
        return cls._build_search_schema()

    GRAPHQL_SEARCH_WITH_REPOS: ClassVar[str] = (
        """
        query SearchWithRepos($q: String!, $after: String) {
          search(query: $q, type: ISSUE, first: 100, after: $after) {
            issueCount
            pageInfo { hasNextPage endCursor }
            nodes {
              ... on Issue { repository { name } }
              ... on PullRequest { repository { name } }
            }
          }
          rateLimit { cost remaining }
        }
        """
    )

    @property
    def query(self) -> str:
        return self.GRAPHQL_SEARCH_WITH_REPOS

    @property
    def authenticator(self) -> WrapperGitHubTokenAuthenticator:
        if self._authenticator is None:
            self._authenticator = WrapperGitHubTokenAuthenticator(stream=self)
        return self._authenticator

    def prepare_request_payload(self, context: Mapping[str, Any] | None, next_page_token: Any | None) -> dict:
        return {"query": self.query, "variables": {"q": "", "after": None}}

    def _get_months_to_process(self) -> list[str]:
        cfg_source = getattr(self, "_search_cfg", None) or self.config
        s = cfg_source.get("search", {})
        backfill = s.get("backfill", {})
        start = backfill.get("start_month")
        if start:
            end = backfill.get("end_month") or get_last_complete_month()
            return month_range(start, end)
        self.logger.info("No backfill configuration found, skipping processing")
        return []

    def _get_bookmark_for_context(self, context: dict) -> str | None:
        try:
            stream_state = self.tap.state.get("bookmarks", {}).get(self.name, {})
            for partition_state in stream_state.get("partitions", []):
                state_context = partition_state.get("context", {})
                if state_context.get("org") == context.get("org") and state_context.get("repo") == context.get("repo"):
                    return partition_state.get("replication_key_value")
            return None
        except Exception:
            return None

    def _should_include_month(self, month_str: str, bookmark_date: date | None) -> bool:
        month_date = month_to_date(month_str)
        last_complete = get_last_complete_month_date()
        if month_date > last_complete:
            return False
        if bookmark_date is None:
            return True
        return month_date > bookmark_date

    def _build_search_query(self, org: str, start_date: str, end_date: str, kind: str) -> str:
        base_query = f"org:{org}"
        if kind == "bug":
            base_query += ' is:issue is:open label:bug,"[type] bug","type: bug"'
        elif kind == "pr":
            base_query += " type:pr is:merged"
        else:
            base_query += " type:issue is:open"
        return f"{base_query} created:{start_date}..{end_date}"

    def _build_repo_search_query(self, repo: str, start_date: str, end_date: str, kind: str) -> str:
        base_query = f"repo:{repo}"
        if kind == "bug":
            base_query += ' is:issue is:open label:bug,"[type] bug","type: bug"'
        elif kind == "pr":
            base_query += " type:pr is:merged"
        else:
            base_query += " type:issue is:open"
        return f"{base_query} created:{start_date}..{end_date}"

    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        now = datetime.utcnow().isoformat() + "Z"
        partitions_to_process = [context] if context else self.partitions
        for partition in partitions_to_process:
            org = partition["org"]
            month = partition["month"]
            query = partition["search_query"]
            api_url_base = partition["api_url_base"]
            repo_breakdown = partition.get("repo_breakdown", False)
            search_name = partition["search_name"]

            if repo_breakdown:
                repo_counts = self._search_with_repo_breakdown(query, api_url_base)
                for repo, count in repo_counts.items():
                    # Keep API calls org-scoped, but emit a repo-scoped query for clarity in output
                    emitted_query = f"{query} repo:{org}/{repo}"
                    yield {
                        "search": search_name,
                        "query": emitted_query,
                        "org": org,
                        "repo": repo,
                        "month": month,
                        "count": count,
                        "updated_at": now,
                    }
            else:
                total_count = self._search_aggregate_count(query, api_url_base)
                repo_name = "aggregate"
                if query.startswith("repo:"):
                    repo_part = query.split(" ")[0]
                    if "/" in repo_part:
                        repo_name = repo_part.split("/")[-1].lower()
                yield {
                    "search": search_name,
                    "query": query,
                    "org": org,
                    "repo": repo_name,
                    "month": month,
                    "count": total_count,
                    "updated_at": now,
                }

    def _search_with_repo_breakdown(self, query: str, api_url_base: str) -> dict[str, int]:
        total_count = self._search_aggregate_count(query, api_url_base)
        return self._compute_repo_counts(query, api_url_base, total_count)

    def _compute_repo_counts(self, query: str, api_url_base: str, total_count: int) -> dict[str, int]:
        if total_count <= 1000:
            return self._get_repo_counts_from_nodes(query, api_url_base)
        return self._search_with_auto_slicing(query, api_url_base)

    def _search_aggregate_count(self, query: str, api_url_base: str) -> int:
        payload = {"query": self.query, "variables": {"q": query, "after": None}}
        prepared_request = self.build_prepared_request(method="POST", url=f"{api_url_base}/graphql", json=payload)
        response = self._request(prepared_request, None)
        response_json = response.json()
        return response_json["data"]["search"]["issueCount"]

    def _get_repo_counts_from_nodes(self, query: str, api_url_base: str) -> dict[str, int]:
        repo_counts: Counter[str] = Counter()
        after = None
        while True:
            payload = {"query": self.query, "variables": {"q": query, "after": after}}
            prepared_request = self.build_prepared_request(method="POST", url=f"{api_url_base}/graphql", json=payload)
            response = self._request(prepared_request, None)
            response_json = response.json()
            search = response_json["data"]["search"]
            for node in search["nodes"]:
                repo_name = node["repository"]["name"]
                repo_counts.update([repo_name])
            if not search["pageInfo"]["hasNextPage"]:
                break
            after = search["pageInfo"]["endCursor"]
        return dict(repo_counts)

    def _search_with_auto_slicing(self, query: str, api_url_base: str) -> dict[str, int]:
        date_match = re.search(r"created:(\d{4}-\d{2}-\d{2})\.\.(\d{4}-\d{2}-\d{2})", query)
        if not date_match:
            return self._get_repo_counts_from_nodes(query, api_url_base)
        start_date = datetime.strptime(date_match.group(1), "%Y-%m-%d")
        end_date = datetime.strptime(date_match.group(2), "%Y-%m-%d")
        repo_counts: Counter[str] = Counter()
        slice_days = int(os.environ.get("GITHUB_SEARCH_SLICE_DAYS", "7")) or 7
        current = start_date
        while current <= end_date:
            slice_end = min(current + timedelta(days=slice_days - 1), end_date)
            slice_query = re.sub(
                r"created:\d{4}-\d{2}-\d{2}\.\.\d{4}-\d{2}-\d{2}",
                f"created:{current.strftime('%Y-%m-%d')}..{slice_end.strftime('%Y-%m-%d')}",
                query,
            )
            slice_counts = self._get_repo_counts_from_nodes(slice_query, api_url_base)
            for repo, count in slice_counts.items():
                repo_counts.update({repo: count})
            current = slice_end + timedelta(days=1)
        return dict(repo_counts)


class ConfigurableSearchCountStream(SearchCountStreamBase):
    def __init__(self, stream_config: dict, tap):
        self.stream_config = stream_config
        self.query_template = stream_config["query_template"]
        self.stream_description = stream_config.get("description", f"Search stream: {stream_config['name']}")
        self.name = f"{stream_config['name']}_search_counts"
        self.stream_type = stream_config.get("stream_type", stream_config.get("name", "custom"))
        self.tap = tap
        super().__init__(tap=tap, name=self.name, schema=self.get_configurable_schema())

    def get_configurable_schema(self) -> dict:
        return self._build_search_schema()

    def _build_search_query(self, org: str, start_date: str, end_date: str, stream_type: str) -> str:
        return self.query_template.format(org=org, start=start_date, end=end_date)

    @property
    def partitions(self) -> list[Context]:
        cfg_source = getattr(self, "_search_cfg", None) or self.config
        s = cfg_source.get("search", {})
        scope = s.get("scope", {})
        orgs = scope.get("orgs", [])
        repos = scope.get("repos", [])
        api_url_base = scope.get("api_url_base", "https://api.github.com")
        breakdown = scope.get("breakdown", "none") == "repo"
        if not orgs and not repos:
            self.logger.warning(f"No orgs or repos provided for {self.name}")
            return []

        partitions: list[Context] = []
        months = self._get_months_to_process()
        stream_name = self.stream_config["name"]

        org_bookmarks: dict[tuple[str, str], date | None] = {}
        for org in orgs:
            bookmark_str = self._get_bookmark_for_context({"org": org})
            org_bookmarks[("single", org)] = month_to_date(bookmark_str) if bookmark_str else None

        repo_bookmarks: dict[tuple[str, str], date | None] = {}
        for repo in repos:
            bookmark_str = self._get_bookmark_for_context({"org": repo, "repo": repo})
            repo_bookmarks[("single", repo)] = month_to_date(bookmark_str) if bookmark_str else None

        for month in months:
            year, month_num = map(int, month.split("-"))
            start_date = f"{year:04d}-{month_num:02d}-01"
            last_day = calendar.monthrange(year, month_num)[1]
            end_date = f"{year:04d}-{month_num:02d}-{last_day:02d}"

            for org in orgs:
                bookmark_date = org_bookmarks.get(("single", org))
                if self._should_include_month(month, bookmark_date):
                    query = self._build_search_query(org, start_date, end_date, self.stream_type)
                    partitions.append({
                        "search_name": stream_name,
                        "search_query": query,
                        "api_url_base": api_url_base,
                        "org": org,
                        "month": month,
                        "kind": stream_name,
                        "repo_breakdown": breakdown,
                    })

            for repo in repos:
                org = repo.split("/")[0]
                bookmark_date = repo_bookmarks.get(("single", repo))
                if self._should_include_month(month, bookmark_date):
                    query = self._build_repo_search_query(repo, start_date, end_date, self.stream_type)
                    partitions.append({
                        "search_name": stream_name,
                        "search_query": query,
                        "api_url_base": api_url_base,
                        "org": org,
                        "month": month,
                        "kind": stream_name,
                        "repo_breakdown": False,
                    })

        self.logger.info(f"Generated {len(partitions)} partitions after incremental filtering")
        return partitions


def validate_stream_config(stream_config: dict) -> list[str]:
    errors = []
    required_fields = ["name", "query_template"]
    for field in required_fields:
        if not stream_config.get(field):
            errors.append(f"Missing required field: {field}")
    name = stream_config.get("name", "")
    if name and not name.replace("_", "").replace("-", "").isalnum():
        errors.append(f"Stream name '{name}' must contain only alphanumeric characters, underscores, and hyphens")
    query_template = stream_config.get("query_template", "")
    for placeholder in ["{org}", "{start}", "{end}"]:
        if placeholder not in query_template:
            errors.append(f"Query template must contain {placeholder} placeholder")
    return errors


def create_configurable_streams(tap, config_override: dict | None = None) -> list:
    streams: list[ConfigurableSearchCountStream] = []
    config = config_override or tap.config
    if "search" in config:
        s = config.get("search", {})
        for sd in s.get("streams", []):
            sc = {
                "name": sd.get("name"),
                "query_template": sd.get("query_template"),
                "description": sd.get("description"),
            }
            errors = validate_stream_config(sc)
            if errors:
                tap.logger.warning(f"Invalid stream config '{sc.get('name', 'unknown')}': {'; '.join(errors)}")
                continue
            streams.append(ConfigurableSearchCountStream(sc, tap))
    else:
        tap.logger.warning("No search configuration found")
    if not streams:
        tap.logger.warning("No search streams created from configuration")
    return streams
