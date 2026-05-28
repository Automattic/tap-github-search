"""GitHub search count streams - simplified Singer SDK implementation (wrapper)."""

from __future__ import annotations

import calendar
import re
from datetime import date, datetime, timedelta
from typing import Any, ClassVar, Iterable, Mapping
import os
import requests
import json
from collections import Counter

import time

from singer_sdk import typing as th
from singer_sdk.exceptions import RetriableAPIError
from singer_sdk.helpers.types import Context

from tap_github.client import GitHubGraphqlStream
from tap_github_search.authenticator import WrapperGitHubTokenAuthenticator

# Essential batching configuration
BATCH_SIZE = int(os.environ.get("TAP_GITHUB_SEARCH_BATCH_SIZE", "100"))
NODES_THRESHOLD = 1000  # Threshold for using nodes approach vs batching

# Regex patterns for query parsing
ORG_PATTERN = re.compile(r"\borg:([^\s]+)")
REPO_PATTERN = re.compile(r"\brepo:([^\s/]+)/([^\s]+)")  
ORG_REPLACEMENT_PATTERN = re.compile(r"\borg:[^\s]+\s*")

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
    selected_by_default: bool = True

    def __init__(self, tap, name=None, schema=None, path=None):
        self.tap = tap
        super().__init__(tap=tap, name=name or self.name, schema=schema or self.get_schema(), path=path)

    _authenticator: WrapperGitHubTokenAuthenticator | None = None

    def _build_repo_query(self, org: str, repo: str, rest_query: str) -> str:
        """Build a repo-scoped search query."""
        return f"repo:{org}/{repo} {rest_query}".strip()

    def _build_graphql_payload(self, query_template: str, variables: dict[str, Any]) -> dict[str, Any]:
        """Build a standardized GraphQL payload."""
        return {"query": query_template, "variables": variables}

    def _make_graphql_request(self, query_template: str, variables: dict[str, Any], api_url_base: str):
        """Make a GraphQL request with standardized payload building."""
        payload = self._build_graphql_payload(query_template, variables)
        graphql_base = api_url_base.rstrip("/").removesuffix("/v3")
        prepared_request = self.build_prepared_request(method="POST", url=f"{graphql_base}/graphql", json=payload)
        decorated_request = self.request_decorator(self._request)
        return decorated_request(prepared_request, None)

    def _filter_active_repos(self, repos: list[dict[str, Any]]) -> list[str]:
        """Filter out forks and archived repos, return list of active repo names."""
        return [
            repo["name"]
            for repo in repos
            if repo and not repo.get("isFork", False) and not repo.get("isArchived", False)
        ]

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

    # Essential GraphQL fix for comma-separated labels
    GRAPHQL_SEARCH_COUNT_ONLY: ClassVar[str] = (
        """
        query SearchCount($q: String!) {
          search(query: $q, type: ISSUE, first: 1) {
            issueCount
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
        stream_state = self.tap.state.get("bookmarks", {}).get(self.name, {})
        for partition_state in stream_state.get("partitions", []):
            state_context = partition_state.get("context", {})
            if state_context.get("org") == context.get("org") and state_context.get("repo") == context.get("repo"):
                return partition_state.get("replication_key_value")

    def _should_include_month(self, month_str: str, bookmark_date: date | None) -> bool:
        month_date = month_to_date(month_str)
        last_complete = get_last_complete_month_date()
        if month_date > last_complete:
            return False
        if bookmark_date is None:
            return True
        return month_date > bookmark_date

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
                    rest_query = ORG_REPLACEMENT_PATTERN.sub("", query).strip()
                    emitted_query = self._build_repo_query(org, repo, rest_query)
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
        """Per-repo breakdown with simple batching optimization."""
        # Handle repo-scoped queries directly
        repo_m = REPO_PATTERN.search(query)
        if repo_m:
            repo_name = repo_m.group(2).lower()
            count = self._search_aggregate_count(query, api_url_base)
            return {repo_name: count}

        # Handle org-scoped queries
        org_m = ORG_PATTERN.search(query)
        if not org_m:
            total_count = self._search_aggregate_count(query, api_url_base)
            return self._compute_repo_counts(query, api_url_base, total_count)

        org = org_m.group(1)
        rest_query = ORG_REPLACEMENT_PATTERN.sub("", query).strip()
        
        total_count = self._search_aggregate_count(query, api_url_base)
        
        if total_count == 0:
            return {}
            
        # Use nodes approach for smaller result sets
        if total_count <= NODES_THRESHOLD:
            return self._get_repo_counts_from_nodes(query, api_url_base)
            
        # Use batching for larger result sets
        repos = self._list_repos_for_org(api_url_base, org)
        return self._get_repo_counts_via_batching(repos, org, rest_query, api_url_base)

    def _get_repo_counts_via_batching(self, repos: list[str], org: str, rest_query: str, api_url_base: str) -> dict[str, int]:
        """Simple batched repo count fetching."""
        if not repos:
            return {}
        
        repo_counts = {}
        self.logger.info(f"Search batching: batch_size={BATCH_SIZE}, total_repos={len(repos)}")
        
        # Process repos in batches
        for i in range(0, len(repos), BATCH_SIZE):
            batch_repos = repos[i:i + BATCH_SIZE]
            queries = [self._build_repo_query(org, repo, rest_query) for repo in batch_repos]
            
            try:
                counts = self._search_aggregate_count_batch(queries, api_url_base)
                for repo_name, count in zip(batch_repos, counts):
                    if count > 0:  # Only include repos with results
                        repo_counts[repo_name] = count
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Batch failed, falling back to individual queries: {str(e)}")
                # Fallback to individual queries
                for repo in batch_repos:
                    query = self._build_repo_query(org, repo, rest_query)
                    count = self._search_aggregate_count(query, api_url_base)
                    if count > 0:
                        repo_counts[repo] = count
                    
        return repo_counts

    def _search_aggregate_count_batch(self, queries: list[str], api_url_base: str) -> list[int]:
        """Batch GraphQL query using aliases - core batching optimization."""
        if not queries:
            return []
            
        # Build GraphQL query with aliases
        var_defs = []
        fields = []
        variables: dict[str, str] = {}
        
        for i, query in enumerate(queries):
            var_name = f"q{i}"
            alias = f"a{i}"
            var_defs.append(f"${var_name}: String!")
            fields.append(f"{alias}: search(query: ${var_name}, type: ISSUE, first: 1) {{ issueCount }}")
            variables[var_name] = query
            
        var_section = ", ".join(var_defs)
        field_section = "\n  ".join(fields)
        doc = f"query RepoCounts({var_section}) {{\n  {field_section}\n}}"
        
        response = self._make_graphql_request(doc, variables, api_url_base)
        data = response.json().get("data", {})
        
        # Extract results in order
        results = []
        for i, _ in enumerate(queries):
            alias = f"a{i}"
            count = int(data.get(alias, {}).get("issueCount", 0))
            results.append(count)
            
        return results

    def _compute_repo_counts(self, query: str, api_url_base: str, total_count: int) -> dict[str, int]:
        if total_count <= NODES_THRESHOLD:
            return self._get_repo_counts_from_nodes(query, api_url_base)
        return self._search_with_auto_slicing(query, api_url_base)

    def _search_aggregate_count(self, query: str, api_url_base: str) -> int:
        """Essential GraphQL variables fix for comma-separated labels."""
        response = self._make_graphql_request(self.GRAPHQL_SEARCH_COUNT_ONLY, {"q": query}, api_url_base)
        response_json = response.json()
        return response_json["data"]["search"]["issueCount"]

    def _get_repo_counts_from_nodes(self, query: str, api_url_base: str) -> dict[str, int]:
        repo_counts: Counter[str] = Counter()
        after = None
        skipped = 0
        while True:
            response = self._make_graphql_request(self.query, {"q": query, "after": after}, api_url_base)
            response_json = response.json()
            search = response_json["data"]["search"]
            for node in search["nodes"]:
                if not node or not node.get("repository"):
                    skipped += 1
                    continue
                repo_name = node["repository"]["name"]
                repo_counts.update([repo_name])
            if not search["pageInfo"]["hasNextPage"]:
                break
            after = search["pageInfo"]["endCursor"]
        if skipped:
            self.logger.warning("Skipped %d null/inaccessible node(s) in search results for query: %s", skipped, query)
        if not repo_counts and skipped:
            raise RuntimeError(
                f"All {skipped} node(s) were skipped for query: {query} -- token may lack repo access"
            )
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

    def _list_repos_for_org(self, api_url_base: str, org: str) -> list[str]:
        """List active repositories for an organization."""
        query = """
        query($org:String!, $after:String){
          organization(login:$org){
            repositories(first:100, after:$after){
              pageInfo { hasNextPage endCursor }
              nodes { 
                name 
                isFork
                isArchived
              }
            }
          }
        }
        """
        names: list[str] = []
        after = None
        
        while True:
            resp = self._make_graphql_request(query, {"org": org, "after": after}, api_url_base)
            data = resp.json()["data"]["organization"]["repositories"]
            
            # Filter out forks and archived repos
            active_repos = self._filter_active_repos(data["nodes"])
            names.extend(active_repos)
            
            if not data["pageInfo"]["hasNextPage"]:
                break
            after = data["pageInfo"]["endCursor"]
            
        return names


class ConfigurableSearchCountStream(SearchCountStreamBase):
    def __init__(self, stream_config: dict, tap):
        self.stream_config = stream_config
        self.query_template = stream_config["query_template"]
        self.stream_description = stream_config.get("description", f"Search stream: {stream_config['name']}")
        self.name = f"{stream_config['name']}_search_counts"
        self.stream_type = stream_config.get("stream_type", stream_config.get("name", "custom"))
        self.tap = tap
        super().__init__(tap=tap, name=self.name, schema=self.get_schema())

    def _build_search_query(self, org: str, start_date: str, end_date: str, stream_type: str) -> str:
        return self.query_template.format(org=org, start=start_date, end=end_date)

    def _build_repo_search_query(self, repo: str, start_date: str, end_date: str, stream_type: str) -> str:
        return self.query_template.format(org=repo, start=start_date, end=end_date)

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


def _instance_from_api_base(api_url_base: str) -> str:
    base = (api_url_base or "").lower()
    if "github.a8c.com" in base:
        return "a8c_ghe"
    if "github.tumblr.net" in base:
        return "tumblr_ghe"
    if "api.github.com" in base or "github.com" in base:
        return "github_com"
    return "unknown"


CLOSED_RANGE_RE = re.compile(r"closed:\d{4}-\d{2}-\d{2}\.\.\d{4}-\d{2}-\d{2}")
CLOSED_CAPTURE_RE = re.compile(r"closed:(\d{4}-\d{2}-\d{2})\.\.(\d{4}-\d{2}-\d{2})")


def _hours_between(created: str | None, closed: str | None) -> float | None:
    if not created or not closed:
        return None
    c0 = datetime.fromisoformat(created.replace("Z", "+00:00"))
    c1 = datetime.fromisoformat(closed.replace("Z", "+00:00"))
    return round((c1 - c0).total_seconds() / 3600.0, 4)


class ConfigurablePrVelocityStream(ConfigurableSearchCountStream):
    """Per-PR velocity stream: one row per closed PR with timing + AI cohort flags.

    Reuses the count stream's transport, auth, monthly partitioning and _search_cfg
    injection; overrides the schema and get_records to emit per-PR rows. The base
    is:closed query is day-sliced to stay under the 1000-result search cap, and AI
    membership is folded into is_ai_authored / is_ai_reviewed booleans before emit.
    """

    replication_method: ClassVar[str] = "FULL_TABLE"
    replication_key = None
    primary_keys: ClassVar[list[str]] = ["instance", "org", "repo", "pr_number"]
    state_partitioning_keys: ClassVar[list[str]] = []

    GRAPHQL_PR_VELOCITY: ClassVar[str] = """
    query PrVelocity($q: String!, $after: String) {
      search(query: $q, type: ISSUE, first: 50, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          ... on PullRequest {
            number title url state isDraft
            createdAt closedAt mergedAt updatedAt
            author { login }
            mergedBy { login }
            repository { name nameWithOwner }
            baseRefName headRefName
            additions deletions changedFiles
            reviewDecision
            comments { totalCount }
            reviews { totalCount }
            commits { totalCount }
            labels(first: 10) { nodes { name } }
          }
        }
      }
      rateLimit { cost remaining }
    }
    """

    GRAPHQL_PR_IDS: ClassVar[str] = """
    query PrIds($q: String!, $after: String) {
      search(query: $q, type: ISSUE, first: 100, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes { ... on PullRequest { number repository { nameWithOwner } } }
      }
      rateLimit { cost remaining }
    }
    """

    def __init__(self, stream_config: dict, tap):
        self.stream_config = stream_config
        self.query_template = stream_config["query_template"]
        self.stream_description = stream_config.get("description", f"PR velocity stream: {stream_config['name']}")
        self.name = stream_config["name"]
        self.stream_type = stream_config.get("stream_type", stream_config.get("name", "pr_velocity"))
        self.tap = tap
        # Skip ConfigurableSearchCountStream.__init__ (it forces the *_search_counts name + count schema).
        SearchCountStreamBase.__init__(self, tap=tap, name=self.name, schema=self.get_schema())

    @classmethod
    def get_schema(cls) -> dict:
        return th.PropertiesList(
            th.Property("instance", th.StringType, required=True),
            th.Property("org", th.StringType, required=True),
            th.Property("repo", th.StringType, required=True),
            th.Property("pr_number", th.IntegerType, required=True),
            th.Property("title", th.StringType),
            th.Property("url", th.StringType),
            th.Property("author_login", th.StringType),
            th.Property("created_at", th.DateTimeType),
            th.Property("merged_at", th.DateTimeType),
            th.Property("closed_at", th.DateTimeType),
            th.Property("updated_at", th.DateTimeType),
            th.Property("hours_to_close", th.NumberType),
            th.Property("outcome", th.StringType),
            th.Property("state", th.StringType),
            th.Property("is_draft", th.BooleanType),
            th.Property("merged_by_login", th.StringType),
            th.Property("base_ref", th.StringType),
            th.Property("head_ref", th.StringType),
            th.Property("additions", th.IntegerType),
            th.Property("deletions", th.IntegerType),
            th.Property("changed_files", th.IntegerType),
            th.Property("review_decision", th.StringType),
            th.Property("comment_count", th.IntegerType),
            th.Property("review_count", th.IntegerType),
            th.Property("commit_count", th.IntegerType),
            th.Property("label_names", th.ArrayType(th.StringType)),
            th.Property("is_ai_authored", th.BooleanType),
            th.Property("is_ai_reviewed", th.BooleanType),
            th.Property("month", th.StringType),
            th.Property("synced_at", th.DateTimeType),
        ).to_dict()

    def _scope(self) -> dict:
        cfg_source = getattr(self, "_search_cfg", None) or self.config
        return cfg_source.get("search", {}).get("scope", {})

    def _robust_graphql(self, query: str, variables: dict, api_url_base: str, tries: int = 8):
        """Outer retry around _make_graphql_request: survives transient 502/gateway
        timeouts on heavy queries (the inner request_decorator retries 5xx a few times;
        this adds patient outer attempts with exponential backoff). Only transient
        transport/5xx errors are retried; 4xx/auth/query errors fail fast."""
        delay = 2.0
        last_exc = None
        for attempt in range(tries):
            try:
                return self._make_graphql_request(query, variables, api_url_base)
            except (RetriableAPIError, requests.exceptions.RequestException) as exc:
                last_exc = exc
                self.logger.warning(
                    "GraphQL request failed (attempt %d/%d): %s; backing off %.1fs",
                    attempt + 1, tries, str(exc)[:140], delay,
                )
                time.sleep(delay)
                delay = min(delay * 1.8, 60.0)
        raise last_exc

    def _iter_pr_nodes(self, query: str, api_url_base: str):
        after = None
        skipped = 0
        seen_any = False
        while True:
            resp = self._robust_graphql(self.GRAPHQL_PR_VELOCITY, {"q": query, "after": after}, api_url_base)
            search = resp.json()["data"]["search"]
            for node in search["nodes"]:
                if not node or node.get("number") is None or not node.get("repository"):
                    skipped += 1
                    continue
                seen_any = True
                yield node
            if not search["pageInfo"]["hasNextPage"]:
                break
            after = search["pageInfo"]["endCursor"]
        if skipped and not seen_any:
            raise RuntimeError(f"All {skipped} node(s) skipped for query: {query} -- token may lack repo access")

    def _collect_pr_ids(self, query: str, api_url_base: str) -> set:
        ids: set = set()
        after = None
        while True:
            resp = self._robust_graphql(self.GRAPHQL_PR_IDS, {"q": query, "after": after}, api_url_base)
            search = resp.json()["data"]["search"]
            for node in search["nodes"]:
                if node and node.get("number") is not None and node.get("repository"):
                    ids.add(f"{node['repository']['nameWithOwner']}#{node['number']}")
            if not search["pageInfo"]["hasNextPage"]:
                break
            after = search["pageInfo"]["endCursor"]
        return ids

    def _iter_day_queries(self, query: str):
        m = CLOSED_CAPTURE_RE.search(query)
        if not m:
            yield query
            return
        start = datetime.strptime(m.group(1), "%Y-%m-%d")
        end = datetime.strptime(m.group(2), "%Y-%m-%d")
        cur = start
        while cur <= end:
            day = cur.strftime("%Y-%m-%d")
            yield CLOSED_RANGE_RE.sub(f"closed:{day}..{day}", query)
            cur += timedelta(days=1)

    def _node_to_row(self, node: dict, instance: str, org: str, month: str, now: str) -> dict:
        repo_full = node["repository"]["nameWithOwner"]
        repo_name = node["repository"].get("name") or repo_full.split("/")[-1]
        merged_at = node.get("mergedAt")
        labels = [l["name"] for l in ((node.get("labels") or {}).get("nodes") or []) if l]
        author = node.get("author") or {}
        merged_by = node.get("mergedBy") or {}
        return {
            "instance": instance,
            "org": org,
            "repo": repo_name,
            "pr_number": node["number"],
            "title": node.get("title"),
            "url": node.get("url"),
            "author_login": author.get("login"),
            "created_at": node.get("createdAt"),
            "merged_at": merged_at,
            "closed_at": node.get("closedAt"),
            "updated_at": node.get("updatedAt"),
            "hours_to_close": _hours_between(node.get("createdAt"), node.get("closedAt")),
            "outcome": "merged" if merged_at else "closed_unmerged",
            "state": node.get("state"),
            "is_draft": node.get("isDraft"),
            "merged_by_login": merged_by.get("login"),
            "base_ref": node.get("baseRefName"),
            "head_ref": node.get("headRefName"),
            "additions": node.get("additions"),
            "deletions": node.get("deletions"),
            "changed_files": node.get("changedFiles"),
            "review_decision": node.get("reviewDecision"),
            "comment_count": (node.get("comments") or {}).get("totalCount"),
            "review_count": (node.get("reviews") or {}).get("totalCount"),
            "commit_count": (node.get("commits") or {}).get("totalCount"),
            "label_names": labels,
            "is_ai_authored": False,
            "is_ai_reviewed": False,
            "month": month,
            "synced_at": now,
        }

    def _process_window(self, window_query, api_url_base, instance, org, month, now, markers, reviewer):
        ai_authored_ids: set = set()
        for marker in markers:
            ai_authored_ids |= self._collect_pr_ids(f"{window_query} {marker}", api_url_base)
        ai_reviewed_ids = self._collect_pr_ids(f"{window_query} {reviewer}", api_url_base) if reviewer else set()
        for node in self._iter_pr_nodes(window_query, api_url_base):
            row = self._node_to_row(node, instance, org, month, now)
            key = f"{node['repository']['nameWithOwner']}#{node['number']}"
            row["is_ai_authored"] = key in ai_authored_ids
            row["is_ai_reviewed"] = key in ai_reviewed_ids
            yield row

    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        now = datetime.utcnow().isoformat() + "Z"
        partitions_to_process = [context] if context else self.partitions
        markers = self.stream_config.get("markers", []) or []
        reviewer = self.stream_config.get("reviewer_clause", "") or ""
        for partition in partitions_to_process:
            org = partition["org"]
            month = partition["month"]
            query = partition["search_query"]
            api_url_base = partition["api_url_base"]
            instance = self._scope().get("instance") or _instance_from_api_base(api_url_base)
            # Adaptive granularity: only day-slice a month when it would exceed the
            # 1000-result search cap. A month with <= cap results is fully retrievable
            # in one windowed (paginated) pass, which avoids days x clauses query blow-up.
            month_count = self._search_aggregate_count(query, api_url_base)
            if month_count == 0:
                continue
            if month_count <= NODES_THRESHOLD:
                yield from self._process_window(query, api_url_base, instance, org, month, now, markers, reviewer)
            else:
                self.logger.info("Month %s for %s has %d > %d results; day-slicing.", month, org, month_count, NODES_THRESHOLD)
                for day_query in self._iter_day_queries(query):
                    yield from self._process_window(day_query, api_url_base, instance, org, month, now, markers, reviewer)


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


def _decode_search_config(tap) -> dict | None:
    """Simple configuration loading from environment variable."""
    search_json = os.getenv("TAP_GITHUB_SEARCH_CONFIG")
    if search_json:
        return json.loads(search_json)


def create_configurable_streams(tap, config_override: dict | None = None) -> list:
    streams: list[ConfigurableSearchCountStream] = []
    config = config_override or tap.config
    
    # Try to get search config from environment variables first
    env_search_config = _decode_search_config(tap)
    if env_search_config:
        tap.logger.info("Using search configuration from environment variables")
        config = dict(config)  # Make a copy
        config["search"] = env_search_config
    
    if "search" in config:
        s = config.get("search", {})
        for sd in s.get("streams", []):
            sc = {
                "name": sd.get("name"),
                "query_template": sd.get("query_template"),
                "description": sd.get("description"),
                "mode": sd.get("mode"),
                "markers": sd.get("markers", []),
                "reviewer_clause": sd.get("reviewer_clause", ""),
            }
            errors = validate_stream_config(sc)
            if errors:
                tap.logger.warning(f"Invalid stream config '{sc.get('name', 'unknown')}': {'; '.join(errors)}")
                continue
            if sd.get("mode") == "pr_velocity":
                streams.append(ConfigurablePrVelocityStream(sc, tap))
            else:
                streams.append(ConfigurableSearchCountStream(sc, tap))
    else:
        tap.logger.warning("No search configuration found")
    if not streams:
        tap.logger.warning("No search streams created from configuration")
    return streams
