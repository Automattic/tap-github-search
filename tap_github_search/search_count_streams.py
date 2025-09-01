"""GitHub search count streams - simplified Singer SDK implementation (wrapper)."""

from __future__ import annotations

import calendar
import re
import time
from datetime import date, datetime, timedelta
from typing import Any, ClassVar, Iterable, Mapping
import os
from collections import Counter

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_github.client import GitHubGraphqlStream
from tap_github_search.authenticator import WrapperGitHubTokenAuthenticator

# Global repo cache shared across all stream instances
_REPO_CACHE: dict[str, list[str]] = {}

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



    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        start_time = time.time()
        now = datetime.utcnow().isoformat() + "Z"
        partitions_to_process = [context] if context else self.partitions
        self.logger.info(f"⏱️ Processing {len(partitions_to_process)} partitions")
        
        for i, partition in enumerate(partitions_to_process):
            partition_start = time.time()
            org = partition["org"]
            month = partition["month"]
            query = partition["search_query"]
            api_url_base = partition["api_url_base"]
            repo_breakdown = partition.get("repo_breakdown", False)
            search_name = partition["search_name"]
            
            self.logger.info(f"⏱️ [{i+1}/{len(partitions_to_process)}] Processing {org} {month} (breakdown={repo_breakdown})")

            if repo_breakdown:
                repo_counts = self._search_with_repo_breakdown(query, api_url_base)
                for repo, count in repo_counts.items():
                    # Keep API calls org-scoped, but emit a repo-scoped query for clarity in output
                    # Replace any org:{org} token with repo:{org}/{repo} so the query is repo-only.
                    rest_query = re.sub(r"\borg:[^\s]+\s*", "", query).strip()
                    emitted_query = f"repo:{org}/{repo} {rest_query}".strip()
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
            
            partition_time = time.time() - partition_start
            self.logger.info(f"⏱️ [{i+1}/{len(partitions_to_process)}] Completed {org} {month} in {partition_time:.1f}s")
        
        total_time = time.time() - start_time
        self.logger.info(f"⏱️ Total processing time: {total_time:.1f}s ({total_time/60:.1f}m)")

    def _search_with_repo_breakdown(self, query: str, api_url_base: str) -> dict[str, int]:
        """
        Per-repo breakdown via aggregate counts: one search per repo using issueCount.
        """
        start_time = time.time()
        self.logger.info(f"⏱️ 🔍 Starting repo breakdown for query: {query[:100]}...")
        
        # If already repo-scoped, just return that repo's aggregate count.
        repo_m = re.search(r"\brepo:([^\s/]+)/([^\s]+)", query)
        if repo_m:
            repo_name = repo_m.group(2).lower()
            count = self._search_aggregate_count(query, api_url_base)
            elapsed = time.time() - start_time
            self.logger.info(f"⏱️ 🔍 Repo-scoped query completed in {elapsed:.1f}s")
            return {repo_name: count}

        # Expect an org-scoped query; fall back to legacy if not found.
        org_m = re.search(r"\borg:([^\s]+)", query)
        if not org_m:
            total_count = self._search_aggregate_count(query, api_url_base)
            result = self._compute_repo_counts(query, api_url_base, total_count)
            elapsed = time.time() - start_time
            self.logger.info(f"⏱️ 🔍 Non-org query completed in {elapsed:.1f}s")
            return result

        org = org_m.group(1)
        rest_query = re.sub(r"\borg:[^\s]+\s*", "", query).strip()
        
        # Optimization: Check total count first to decide strategy
        count_start = time.time()
        total_count = self._search_aggregate_count(query, api_url_base)
        count_time = time.time() - count_start
        self.logger.info(f"⏱️ 🔍 Total count check: {total_count} results in {count_time:.1f}s")
        
        if total_count == 0:
            elapsed = time.time() - start_time
            self.logger.info(f"⏱️ 🔍 Zero results, completed in {elapsed:.1f}s")
            return {}
            
        # For small datasets, get repo names from search results (faster)
        if total_count <= 1000:
            self.logger.info(f"⏱️ 🔍 Using nodes approach for {total_count} results")
            result = self._get_repo_counts_from_nodes(query, api_url_base)
            elapsed = time.time() - start_time
            self.logger.info(f"⏱️ 🔍 Small dataset completed in {elapsed:.1f}s")
            return result
            
        # For large datasets, use optimized approach: first identify active repos from search results
        self.logger.info(f"⏱️ 🔍 Using optimized repo filtering for {total_count} results")
        active_repos = self._get_active_repos_from_search(query, api_url_base)
        self.logger.info(f"⏱️ 🎯 Found {len(active_repos)} active repos with matching issues")
        
        if not active_repos:
            elapsed = time.time() - start_time
            self.logger.info(f"⏱️ 🔍 No active repos found in {elapsed:.1f}s")
            return {}
        
        result = self._get_repo_counts_via_batching(active_repos, org, rest_query, api_url_base)
        elapsed = time.time() - start_time
        self.logger.info(f"⏱️ 🔍 Large dataset completed in {elapsed:.1f}s")
        return result
            
    def _get_repo_counts_via_batching(self, repos: list[str], org: str, rest_query: str, api_url_base: str) -> dict[str, int]:
        """Helper method for batched repo count fetching."""
        start_time = time.time()
        counts: dict[str, int] = {}
        if not repos:
            return counts
            
        batch_size = int(os.environ.get("GITHUB_SEARCH_BATCH_SIZE", "20")) or 20
        self.logger.info(f"⏱️ 📦 Batching {len(repos)} repos with batch_size={batch_size}")
        
        repo_names: list[str] = []
        queries: list[str] = []
        for name in repos:
            repo_names.append(name)
            queries.append(f"repo:{org}/{name} {rest_query}".strip())

        total_batches = (len(queries) + batch_size - 1) // batch_size
        for batch_idx, i in enumerate(range(0, len(queries), batch_size)):
            batch_start = time.time()
            q_batch = queries[i : i + batch_size]
            n_batch = repo_names[i : i + batch_size]
            
            batch_counts = self._search_aggregate_count_batch(q_batch, api_url_base)
            
            batch_results = 0
            for repo_name, c in zip(n_batch, batch_counts):
                if c:
                    counts[repo_name] = c
                    batch_results += 1
                    
            batch_time = time.time() - batch_start
            self.logger.info(f"⏱️ 📦 Batch {batch_idx+1}/{total_batches}: {batch_results} repos with results in {batch_time:.1f}s")
        
        elapsed = time.time() - start_time
        total_results = len([c for c in counts.values() if c > 0])
        self.logger.info(f"⏱️ 📦 Batching completed: {total_results} repos with results in {elapsed:.1f}s")
        return counts

    def _search_aggregate_count_batch(self, queries: list[str], api_url_base: str) -> list[int]:
        """
        Fetch aggregate issueCount for multiple queries in one GraphQL request via aliases.
        Returns counts in the same order as input queries.
        """
        if not queries:
            return []
        # Build GraphQL with variables $q0, $q1, ... and aliases a0, a1, ...
        var_defs = []
        fields = []
        variables: dict[str, str] = {}
        for idx, q in enumerate(queries):
            var_name = f"q{idx}"
            alias = f"a{idx}"
            var_defs.append(f"${var_name}: String!")
            fields.append(f"{alias}: search(query: ${var_name}, type: ISSUE, first: 1) {{ issueCount }}")
            variables[var_name] = q
        var_section = ", ".join(var_defs)
        field_section = "\n  ".join(fields)
        doc = f"query RepoCounts({var_section}) {{\n  {field_section}\n}}"
        payload = {"query": doc, "variables": variables}
        prepared_request = self.build_prepared_request(method="POST", url=f"{api_url_base}/graphql", json=payload)
        response = self._request(prepared_request, None)
        data = response.json().get("data", {})
        results: list[int] = []
        for idx in range(len(queries)):
            alias = f"a{idx}"
            results.append(int(data.get(alias, {}).get("issueCount", 0)))
        return results

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
        
        # Try batched approach first for better performance
        try:
            return self._search_with_auto_slicing_batched(query, api_url_base)
        except Exception as e:
            self.logger.warning(f"Batched slicing failed, falling back to individual queries: {e}")
            return self._search_with_auto_slicing_individual(query, api_url_base)
    
    def _search_with_auto_slicing_batched(self, query: str, api_url_base: str) -> dict[str, int]:
        """Batched version of auto-slicing for better performance."""
        date_match = re.search(r"created:(\d{4}-\d{2}-\d{2})\.\.(\d{4}-\d{2}-\d{2})", query)
        if not date_match:
            return self._get_repo_counts_from_nodes(query, api_url_base)
            
        start_date = datetime.strptime(date_match.group(1), "%Y-%m-%d")
        end_date = datetime.strptime(date_match.group(2), "%Y-%m-%d")
        slice_days = int(os.environ.get("GITHUB_SEARCH_SLICE_DAYS", "5")) or 5
        
        # Generate all slice queries upfront
        slice_queries: list[str] = []
        current = start_date
        while current <= end_date:
            slice_end = min(current + timedelta(days=slice_days - 1), end_date)
            slice_query = re.sub(
                r"created:\d{4}-\d{2}-\d{2}\.\.\d{4}-\d{2}-\d{2}",
                f"created:{current.strftime('%Y-%m-%d')}..{slice_end.strftime('%Y-%m-%d')}",
                query,
            )
            slice_queries.append(slice_query)
            current = slice_end + timedelta(days=1)
        
        # Get aggregate counts for all slices in batched calls
        batch_size = int(os.environ.get("GITHUB_SEARCH_BATCH_SIZE", "20")) or 20
        repo_counts: Counter[str] = Counter()
        
        for i in range(0, len(slice_queries), batch_size):
            batch_queries = slice_queries[i:i + batch_size]
            batch_counts = self._search_aggregate_count_batch(batch_queries, api_url_base)
            
            # For each slice, get repo breakdown from nodes if count > 0
            for slice_query, total_count in zip(batch_queries, batch_counts):
                if total_count > 0:
                    if total_count <= 1000:
                        slice_repo_counts = self._get_repo_counts_from_nodes(slice_query, api_url_base)
                    else:
                        # If still too large, fall back to individual approach for this slice
                        slice_repo_counts = self._search_with_auto_slicing_individual(slice_query, api_url_base)
                    
                    for repo, count in slice_repo_counts.items():
                        repo_counts[repo] += count
        
        return dict(repo_counts)
    
    def _search_with_auto_slicing_individual(self, query: str, api_url_base: str) -> dict[str, int]:
        """Original individual query version as fallback."""
        date_match = re.search(r"created:(\d{4}-\d{2}-\d{2})\.\.(\d{4}-\d{2}-\d{2})", query)
        if not date_match:
            return self._get_repo_counts_from_nodes(query, api_url_base)
        start_date = datetime.strptime(date_match.group(1), "%Y-%m-%d")
        end_date = datetime.strptime(date_match.group(2), "%Y-%m-%d")
        repo_counts: Counter[str] = Counter()
        slice_days = int(os.environ.get("GITHUB_SEARCH_SLICE_DAYS", "5")) or 5
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
                repo_counts[repo] += count
            current = slice_end + timedelta(days=1)
        return dict(repo_counts)

    def _list_repos_for_org(self, api_url_base: str, org: str) -> list[str]:
        if org in _REPO_CACHE:
            self.logger.info(f"⏱️ 💾 Using cached repo list for {org}: {len(_REPO_CACHE[org])} repos")
            return _REPO_CACHE[org]
            
        start_time = time.time()
        self.logger.info(f"⏱️ 📋 Fetching repo list for {org}...")
        
        q = (
            """
        query($org:String!, $after:String){
          organization(login:$org){
            repositories(first:100, after:$after){
              pageInfo { hasNextPage endCursor }
              nodes { name }
            }
          }
        }
        """
        )
        names: list[str] = []
        after = None
        page_count = 0
        while True:
            page_start = time.time()
            payload = {"query": q, "variables": {"org": org, "after": after}}
            prepared = self.build_prepared_request(method="POST", url=f"{api_url_base}/graphql", json=payload)
            resp = self._request(prepared, None)
            data = resp.json()["data"]["organization"]["repositories"]
            page_repos = [n["name"] for n in data["nodes"]]
            names.extend(page_repos)
            page_count += 1
            page_time = time.time() - page_start
            self.logger.info(f"⏱️ 📋 Page {page_count}: {len(page_repos)} repos in {page_time:.1f}s")
            
            if not data["pageInfo"]["hasNextPage"]:
                break
            after = data["pageInfo"]["endCursor"]
            
        elapsed = time.time() - start_time
        self.logger.info(f"⏱️ 📋 Repo listing completed: {len(names)} repos in {elapsed:.1f}s")
        _REPO_CACHE[org] = names
        return names

    def _get_active_repos_from_search(self, query: str, api_url_base: str) -> list[str]:
        """
        Get list of repos that have at least one matching issue by scanning search results.
        This avoids having to query every single repo in the org.
        """
        start_time = time.time()
        self.logger.info(f"⏱️ 🎯 Scanning search results to identify active repos...")
        
        repo_names = set()
        after = None
        pages_scanned = 0
        
        # Scan through search results to collect unique repo names
        while True:
            payload = {"query": self.query, "variables": {"q": query, "after": after}}
            prepared_request = self.build_prepared_request(method="POST", url=f"{api_url_base}/graphql", json=payload)
            response = self._request(prepared_request, None)
            response_json = response.json()
            search = response_json["data"]["search"]
            
            # Extract repo names from search results
            for node in search["nodes"]:
                repo_name = node["repository"]["name"]
                repo_names.add(repo_name)
            
            pages_scanned += 1
            self.logger.info(f"⏱️ 🎯 Page {pages_scanned}: found {len(repo_names)} unique active repos so far")
            
            if not search["pageInfo"]["hasNextPage"]:
                break
            after = search["pageInfo"]["endCursor"]
            
            # Safety limit to avoid infinite loops
            if pages_scanned >= 50:  # 50 pages * 100 results = 5000 max results
                self.logger.warning(f"⏱️ 🎯 Reached safety limit of {pages_scanned} pages")
                break
        
        active_repos = list(repo_names)
        elapsed = time.time() - start_time
        self.logger.info(f"⏱️ 🎯 Active repo scan completed: {len(active_repos)} repos in {elapsed:.1f}s from {pages_scanned} pages")
        
        return active_repos


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
        # For repo-specific queries, use full repo name in place of {org}
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
