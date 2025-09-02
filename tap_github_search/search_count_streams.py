"""GitHub search count streams - simplified Singer SDK implementation (wrapper)."""

from __future__ import annotations

import calendar
import re
from datetime import date, datetime, timedelta
from typing import Any, ClassVar, Iterable, Mapping
import os
import base64
import json
from collections import Counter
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_github.client import GitHubGraphqlStream
from tap_github_search.authenticator import WrapperGitHubTokenAuthenticator

OPTIMAL_BATCH_SIZE = 140
FALLBACK_BATCH_SIZE = 50
HTTP_POOL_CONNECTIONS = 10
HTTP_POOL_MAX_SIZE = 20

_HTTP_SESSION: requests.Session | None = None

def _get_http_session() -> requests.Session:
    """Get or create a global HTTP session with connection pooling and retry strategy."""
    global _HTTP_SESSION
    if _HTTP_SESSION is None:
        _HTTP_SESSION = requests.Session()
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=HTTP_POOL_CONNECTIONS,
            pool_maxsize=HTTP_POOL_MAX_SIZE,
        )
        
        _HTTP_SESSION.mount("https://", adapter)
        _HTTP_SESSION.mount("http://", adapter)
        
    return _HTTP_SESSION

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

    def _request(self, prepared_request, context):
        """Override to use global HTTP session with connection pooling."""
        session = _get_http_session()
        
        if self.authenticator:
            prepared_request.headers.update(self.authenticator.auth_headers or {})
            
        try:
            response = session.send(prepared_request, timeout=30)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 502:
                self.logger.warning(f"502 Bad Gateway error")
            raise

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
        now = datetime.utcnow().isoformat() + "Z"
        partitions_to_process = [context] if context else self.partitions
        
        for i, partition in enumerate(partitions_to_process):
            org = partition["org"]
            month = partition["month"]
            query = partition["search_query"]
            api_url_base = partition["api_url_base"]
            repo_breakdown = partition.get("repo_breakdown", False)
            search_name = partition["search_name"]
            

            if repo_breakdown:
                repo_counts = self._search_with_repo_breakdown(query, api_url_base)
                for repo, count in repo_counts.items():
                    rest_query = re.sub(r"\borg:[^\s]+\s*", "", query).strip()
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
        """
        Per-repo breakdown via aggregate counts: one search per repo using issueCount.
        """
        
        repo_m = re.search(r"\brepo:([^\s/]+)/([^\s]+)", query)
        if repo_m:
            repo_name = repo_m.group(2).lower()
            count = self._search_aggregate_count(query, api_url_base)
            return {repo_name: count}

        org_m = re.search(r"\borg:([^\s]+)", query)
        if not org_m:
            total_count = self._search_aggregate_count(query, api_url_base)
            result = self._compute_repo_counts(query, api_url_base, total_count)
            return result

        org = org_m.group(1)
        rest_query = re.sub(r"\borg:[^\s]+\s*", "", query).strip()
        
        total_count = self._search_aggregate_count(query, api_url_base)
        
        if total_count == 0:
            return {}
            
        if total_count <= 1000:
            result = self._get_repo_counts_from_nodes(query, api_url_base)
            return result
            
        repos = self._list_repos_for_org(api_url_base, org)
        
        result = self._get_repo_counts_via_batching(repos, org, rest_query, api_url_base)
        return result
    
    def _prefilter_repos_with_results(self, repos: list[str], org: str, rest_query: str, api_url_base: str) -> tuple[list[str], dict[str, int]]:
        """Pre-filter repos to only include those with matching issues and return their counts."""
        if not repos:
            return [], {}
            
        prefilter_batch_size = OPTIMAL_BATCH_SIZE
        active_repos = []
        repo_counts = {}
        total_batches = (len(repos) + prefilter_batch_size - 1) // prefilter_batch_size
        
        
        for batch_idx, i in enumerate(range(0, len(repos), prefilter_batch_size)):
            batch_repos = repos[i:i + prefilter_batch_size]
            queries = [self._build_repo_query(org, name, rest_query) for name in batch_repos]
            
            try:
                counts = self._search_aggregate_count_batch(queries, api_url_base)
                for repo_name, count in zip(batch_repos, counts):
                    if count > 0:
                        active_repos.append(repo_name)
                        repo_counts[repo_name] = count
                    else:
                        repo_counts[repo_name] = 0
            except requests.exceptions.HTTPError as e:
                if e.response and e.response.status_code == 502:
                    self.logger.warning(f"502 Bad Gateway - retrying with smaller batch size")
                    
                    batch_results = []
                    for j in range(0, len(batch_repos), FALLBACK_BATCH_SIZE):
                        small_batch = batch_repos[j:j + FALLBACK_BATCH_SIZE]
                        small_queries = [self._build_repo_query(org, name, rest_query) for name in small_batch]
                        try:
                            small_counts = self._search_aggregate_count_batch(small_queries, api_url_base)
                            batch_results.extend(list(zip(small_batch, small_counts)))
                        except Exception:
                            batch_results.extend([(name, 0) for name in small_batch])
                    
                    for repo_name, count in batch_results:
                        if count > 0:
                            active_repos.append(repo_name)
                            repo_counts[repo_name] = count
                        else:
                            repo_counts[repo_name] = 0
                else:
                    self.logger.warning(f"Pre-filter batch failed: {str(e)}")
                    for repo_name in batch_repos:
                        active_repos.append(repo_name)
                        repo_counts[repo_name] = 0
            except Exception as e:
                self.logger.warning(f"Pre-filter batch failed: {str(e)}")
                for repo_name in batch_repos:
                    active_repos.append(repo_name)
                    repo_counts[repo_name] = 0
                
        return active_repos, repo_counts
            
    def _get_repo_counts_via_batching(self, repos: list[str], org: str, rest_query: str, api_url_base: str) -> dict[str, int]:
        """Helper method for batched repo count fetching - now uses pre-filtering results directly."""
        if not repos:
            return {}
        
        active_repos, counts = self._prefilter_repos_with_results(repos, org, rest_query, api_url_base)
        
        if not active_repos:
            return {}, {}
            return {}
            
        
        total_results = len([c for c in counts.values() if c > 0])
        return counts

    def _search_aggregate_count_batch(self, queries: list[str], api_url_base: str) -> list[int]:
        """
        Fetch aggregate issueCount for multiple queries in one GraphQL request via aliases.
        Returns counts in the same order as input queries.
        """
        if not queries:
            return []
            
        var_defs = []
        fields = []
        variables: dict[str, str] = {}
        for batch_idx, query in enumerate(queries):
            var_name = f"q{batch_idx}"
            alias = f"a{batch_idx}"
            var_defs.append(f"${var_name}: String!")
            fields.append(f"{alias}: search(query: ${var_name}, type: ISSUE, first: 1) {{ issueCount }}")
            variables[var_name] = query
            
        var_section = ", ".join(var_defs)
        field_section = "\n  ".join(fields)
        doc = f"query RepoCounts({var_section}) {{\n  {field_section}\n}}"
        payload = {"query": doc, "variables": variables}
        prepared_request = self.build_prepared_request(method="POST", url=f"{api_url_base}/graphql", json=payload)
        response = self._request(prepared_request, None)
        data = response.json().get("data", {})
        
        results = []
        for batch_idx, query in enumerate(queries):
            alias = f"a{batch_idx}"
            count = int(data.get(alias, {}).get("issueCount", 0))
            results.append(count)
            
        return results

    def _compute_repo_counts(self, query: str, api_url_base: str, total_count: int) -> dict[str, int]:
        if total_count <= 1000:
            return self._get_repo_counts_from_nodes(query, api_url_base)
        return self._search_with_auto_slicing(query, api_url_base)

    def _search_aggregate_count(self, query: str, api_url_base: str) -> int:
        payload = {"query": self.GRAPHQL_SEARCH_COUNT_ONLY, "variables": {"q": query}}
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
        
        return self._search_with_auto_slicing_individual(query, api_url_base)
    
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
        
        q = (
            """
        query($org:String!, $after:String){
          organization(login:$org){
            repositories(first:100, after:$after){
              pageInfo { hasNextPage endCursor }
              nodes { 
                name 
                isFork
                isArchived
                isMirror
                isTemplate
              }
            }
          }
        }
        """
        )
        names: list[str] = []
        after = None
        page_count = 0
        total_excluded = {"forks": 0, "archived": 0, "mirrors": 0, "templates": 0}
        while True:
            payload = {"query": q, "variables": {"org": org, "after": after}}
            prepared = self.build_prepared_request(method="POST", url=f"{api_url_base}/graphql", json=payload)
            resp = self._request(prepared, None)
            data = resp.json()["data"]["organization"]["repositories"]
            all_page_repos = data["nodes"]
            filtered_repos = []
            excluded_count = {"forks": 0, "archived": 0, "mirrors": 0, "templates": 0}
            
            for repo in all_page_repos:
                if repo.get("isFork", False):
                    excluded_count["forks"] += 1
                elif repo.get("isArchived", False):
                    excluded_count["archived"] += 1
                elif repo.get("isMirror", False):
                    excluded_count["mirrors"] += 1
                elif repo.get("isTemplate", False):
                    excluded_count["templates"] += 1
                else:
                    filtered_repos.append(repo["name"])
            
            names.extend(filtered_repos)
            page_count += 1
            
            for key in total_excluded:
                total_excluded[key] += excluded_count[key]
            
            excluded_total = sum(excluded_count.values())
            
            if not data["pageInfo"]["hasNextPage"]:
                break
            after = data["pageInfo"]["endCursor"]
            
        total_excluded_count = sum(total_excluded.values())
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
    """Decode search configuration from Base64, file, or direct JSON."""
    # Try Base64 decoding first (Option B)
    b64_search = os.getenv("TAP_GITHUB_SEARCH_STATS_SEARCH_B64")
    if b64_search:
        try:
            decoded = base64.b64decode(b64_search).decode("utf-8")
            return json.loads(decoded)
        except Exception as e:
            tap.logger.warning(f"Failed to decode Base64 search config: {e}")
    
    # Try file loading (Option A - future enhancement)
    search_file = os.getenv("TAP_GITHUB_SEARCH_STATS_SEARCH_FILE")
    if search_file:
        try:
            with open(search_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            tap.logger.warning(f"Failed to load search config from file {search_file}: {e}")
    
    # Fallback to direct JSON from env var (legacy)
    search_json = os.getenv("TAP_GITHUB_SEARCH_STATS_SEARCH")
    if search_json:
        try:
            return json.loads(search_json)
        except Exception as e:
            tap.logger.warning(f"Failed to parse direct search config JSON: {e}")
    
    return None


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
