"""PR velocity stream for GitHub search results."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, ClassVar, Iterable, Iterator

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_github_search.search_count_streams import (
    ConfigurableSearchCountStream,
    NODES_THRESHOLD,
    SearchCountStreamBase,
)

DEFAULT_INSTANCE = "github_com"
CLOSED_CAPTURE_RE = re.compile(r"closed:(\d{4}-\d{2}-\d{2})\.\.(\d{4}-\d{2}-\d{2})")
CREATED_QUALIFIER_RE = re.compile(r"(?:^|\s)-?created:")
ORG_CAPTURE_RE = re.compile(r"(?:^|\s)org:([^\s]+)")
ORG_QUALIFIER_RE = re.compile(r"(?:^|\s)org:[^\s]+\s*")
REPO_CAPTURE_RE = re.compile(r"(?:^|\s)repo:([^\s/]+)/([^\s]+)")
CREATED_SEARCH_START_DATE = date(2008, 1, 1)


def _hours_between(created: str | None, closed: str | None) -> float | None:
    if not created or not closed:
        return None
    created_at = datetime.fromisoformat(created.replace("Z", "+00:00"))
    closed_at = datetime.fromisoformat(closed.replace("Z", "+00:00"))
    return round((closed_at - created_at).total_seconds() / 3600.0, 4)


def _body_contains_marker(body_text: str | None, marker: str) -> bool:
    literal = marker.strip().strip("'\"")
    return bool(literal and literal.casefold() in (body_text or "").casefold())


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


class ConfigurablePrVelocityStream(ConfigurableSearchCountStream):
    """One row per closed PR with timing and AI cohort flags."""

    replication_method: ClassVar[str] = "INCREMENTAL"
    replication_key = "month"
    primary_keys: ClassVar[list[str]] = ["instance", "org_", "repo", "pr_number"]
    state_partitioning_keys: ClassVar[list[str]] = ["org", "repo"]

    GRAPHQL_PR_VELOCITY: ClassVar[str] = """
    query PrVelocity($q: String!, $after: String) {
      search(query: $q, type: ISSUE, first: 25, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          ... on PullRequest {
            number bodyText
            createdAt closedAt mergedAt
            author { login }
            repository { name nameWithOwner }
          }
        }
      }
      rateLimit { cost remaining }
    }
    """

    GRAPHQL_PR_IDS: ClassVar[str] = """
    query PrIds($q: String!, $after: String) {
      search(query: $q, type: ISSUE, first: 50, after: $after) {
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
        self._org_repo_cache: dict[tuple[str, str], list[str]] = {}
        SearchCountStreamBase.__init__(self, tap=tap, name=self.name, schema=self.get_schema())

    @classmethod
    def get_schema(cls) -> dict:
        return th.PropertiesList(
            th.Property("instance", th.StringType, required=True),
            th.Property("org_", th.StringType, required=True),
            th.Property("repo", th.StringType, required=True),
            th.Property("pr_number", th.IntegerType, required=True),
            th.Property("author_login", th.StringType),
            th.Property("created_at", th.DateTimeType),
            th.Property("closed_at", th.DateTimeType),
            th.Property("merged_at", th.DateTimeType),
            th.Property("hours_to_close", th.NumberType),
            th.Property("outcome", th.StringType),
            th.Property("is_ai_authored", th.BooleanType),
            th.Property("is_ai_reviewed", th.BooleanType),
            th.Property("month", th.StringType),
            th.Property("synced_at", th.DateTimeType),
        ).to_dict()

    def _iter_pr_nodes(self, query: str, api_url_base: str):
        after = None
        has_next = True
        while has_next:
            resp = self._make_graphql_request(self.GRAPHQL_PR_VELOCITY, {"q": query, "after": after}, api_url_base)
            search = resp.json()["data"]["search"]
            page_info = search["pageInfo"]
            for node in search["nodes"]:
                if node and node.get("number") is not None and node.get("repository"):
                    yield node
            has_next = page_info["hasNextPage"]
            after = page_info["endCursor"]

    def _collect_pr_ids(self, query: str, api_url_base: str) -> set[str]:
        ids: set[str] = set()
        after = None
        has_next = True
        while has_next:
            resp = self._make_graphql_request(self.GRAPHQL_PR_IDS, {"q": query, "after": after}, api_url_base)
            search = resp.json()["data"]["search"]
            page_info = search["pageInfo"]
            for node in search["nodes"]:
                if node and node.get("number") is not None and node.get("repository"):
                    ids.add(f"{node['repository']['nameWithOwner']}#{node['number']}")
            has_next = page_info["hasNextPage"]
            after = page_info["endCursor"]
        return ids

    def _list_repos_for_pr_velocity(self, api_url_base: str, org: str) -> list[str]:
        """Preserve org: search semantics by including archived and forked repos."""
        cache_key = (api_url_base, org)
        if cache_key in self._org_repo_cache:
            return self._org_repo_cache[cache_key]

        repos = self._list_repos_for_org(api_url_base, org, include_inactive=True)
        self._org_repo_cache[cache_key] = repos
        return repos

    def _iter_day_queries(self, query: str) -> Iterator[str]:
        match = CLOSED_CAPTURE_RE.search(query)
        if not match:
            yield query
            return
        current = date.fromisoformat(match.group(1))
        end = date.fromisoformat(match.group(2))
        while current <= end:
            day = current.isoformat()
            yield query.replace(match.group(0), f"closed:{day}..{day}")
            current += timedelta(days=1)

    def _iter_processable_queries(
        self,
        query: str,
        api_url_base: str,
        org: str,
    ) -> Iterator[str]:
        month_count = self._search_aggregate_count(query, api_url_base)
        if month_count == 0:
            return
        if month_count <= NODES_THRESHOLD:
            yield query
            return

        for day_query in self._iter_day_queries(query):
            day_count = self._search_aggregate_count(day_query, api_url_base)
            if day_count == 0:
                continue
            if day_count <= NODES_THRESHOLD:
                yield day_query
                continue
            yield from self._iter_repo_queries_for_capped_day(
                day_query,
                api_url_base,
                org,
                day_count,
            )

    def _iter_repo_queries_for_capped_day(
        self,
        day_query: str,
        api_url_base: str,
        org: str,
        day_count: int,
    ) -> Iterator[str]:
        repo_match = REPO_CAPTURE_RE.search(day_query)
        if repo_match:
            repo_label = f"{repo_match.group(1)}/{repo_match.group(2)}"
            yield from self._iter_created_range_queries_for_capped_day(
                day_query,
                api_url_base,
                repo_label,
                day_count,
            )
            return

        org_match = ORG_CAPTURE_RE.search(day_query)
        if not org_match:
            raise RuntimeError(
                "pr_velocity repo-scoped day window exceeds GitHub search cap: "
                f"GitHub GraphQL search returns at most {NODES_THRESHOLD} "
                "results per query, and this query is already scoped below "
                f"an organization. Query: {day_query}"
            )

        org = org_match.group(1)
        repos = self._list_repos_for_pr_velocity(api_url_base, org)
        if not repos:
            raise RuntimeError(
                "Could not split capped pr_velocity day query because "
                f"org '{org}' has no repositories"
            )

        rest_query = ORG_QUALIFIER_RE.sub(" ", day_query).strip()
        repo_counts = self._get_repo_counts_via_batching(
            repos,
            org,
            rest_query,
            api_url_base,
        )
        self._ensure_split_count_matches(
            "repo",
            day_query,
            expected=day_count,
            actual=sum(repo_counts.values()),
        )
        for repo, count in repo_counts.items():
            repo_query = self._build_repo_query(org, repo, rest_query)
            if count > NODES_THRESHOLD:
                yield from self._iter_created_range_queries_for_capped_day(
                    repo_query,
                    api_url_base,
                    f"{org}/{repo}",
                    count,
                )
                continue
            yield repo_query

    def _iter_created_range_queries_for_capped_day(
        self,
        repo_query: str,
        api_url_base: str,
        repo_label: str,
        repo_day_count: int,
    ) -> Iterator[str]:
        if CREATED_QUALIFIER_RE.search(repo_query):
            raise RuntimeError(
                "pr_velocity created-date window exceeds GitHub search cap: "
                f"GitHub GraphQL search returns at most {NODES_THRESHOLD} "
                "results per query, and this query is already scoped by "
                f"created date. Query: {repo_query}"
            )

        match = CLOSED_CAPTURE_RE.search(repo_query)
        if not match or match.group(1) != match.group(2):
            raise RuntimeError(
                "Could not split capped pr_velocity repo query by created date "
                f"because it is not scoped to a single closed day. Query: {repo_query}"
            )

        closed_day = date.fromisoformat(match.group(1))
        self.logger.info(
            "Repo-scoped pr_velocity day exceeded search cap; splitting by "
            f"created date for repo '{repo_label}'"
        )
        created_query = self._append_created_range(
            repo_query,
            CREATED_SEARCH_START_DATE,
            closed_day,
        )
        created_count = self._search_aggregate_count(created_query, api_url_base)
        self._ensure_split_count_matches(
            "created-date",
            created_query,
            expected=repo_day_count,
            actual=created_count,
        )
        yield from self._iter_created_range_queries(
            repo_query,
            api_url_base,
            repo_label,
            CREATED_SEARCH_START_DATE,
            closed_day,
            created_count,
        )

    def _iter_created_range_queries(
        self,
        base_query: str,
        api_url_base: str,
        repo_label: str,
        start: date,
        end: date,
        count: int,
    ) -> Iterator[str]:
        if count == 0:
            return

        query = self._append_created_range(base_query, start, end)
        if count <= NODES_THRESHOLD:
            yield query
            return

        # No narrower date-only split exists for one repo on one created day.
        if start >= end:
            raise RuntimeError(
                "pr_velocity repo created-day window exceeds GitHub search cap: "
                f"GitHub GraphQL search returns at most {NODES_THRESHOLD} "
                "results per query, "
                f"but repo '{repo_label}' matched {count}. Query: {query}"
            )

        midpoint = start + timedelta(days=(end - start).days // 2)
        right_start = midpoint + timedelta(days=1)

        for child_start, child_end in ((start, midpoint), (right_start, end)):
            child_query = self._append_created_range(base_query, child_start, child_end)
            child_count = self._search_aggregate_count(child_query, api_url_base)
            yield from self._iter_created_range_queries(
                base_query,
                api_url_base,
                repo_label,
                child_start,
                child_end,
                child_count,
            )

    def _append_created_range(self, query: str, start: date, end: date) -> str:
        return f"{query} created:{start.isoformat()}..{end.isoformat()}"

    def _ensure_split_count_matches(
        self,
        split_name: str,
        query: str,
        *,
        expected: int,
        actual: int,
    ) -> None:
        if actual == expected:
            return
        # Historical counts should be stable; near-live windows may fail loudly
        # if GitHub search counts change between parent and child queries.
        raise RuntimeError(
            f"pr_velocity {split_name} split did not preserve GitHub search "
            f"count: parent matched {expected}, split matched {actual}. "
            f"Query: {query}"
        )

    def _node_to_row(self, node: dict, instance: str, org: str, month: str, now: str) -> dict:
        repo_name = node["repository"].get("name") or node["repository"]["nameWithOwner"].split("/")[-1]
        merged_at = node.get("mergedAt")
        author = node.get("author") or {}
        return {
            "instance": instance,
            "org_": org,
            "repo": repo_name,
            "pr_number": node["number"],
            "author_login": author.get("login"),
            "created_at": node.get("createdAt"),
            "closed_at": node.get("closedAt"),
            "merged_at": merged_at,
            "hours_to_close": _hours_between(node.get("createdAt"), node.get("closedAt")),
            "outcome": "merged" if merged_at else "closed_unmerged",
            "month": month,
            "synced_at": now,
        }

    def _process_window(self, window_query, api_url_base, instance, org, month, now, markers, reviewer):
        ai_reviewed_ids = self._collect_pr_ids(f"{window_query} {reviewer}", api_url_base) if reviewer else set()
        for node in self._iter_pr_nodes(window_query, api_url_base):
            row = self._node_to_row(node, instance, org, month, now)
            key = f"{node['repository']['nameWithOwner']}#{node['number']}"
            row["is_ai_authored"] = any(_body_contains_marker(node.get("bodyText"), marker) for marker in markers)
            row["is_ai_reviewed"] = key in ai_reviewed_ids
            yield row

    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        now = _utc_now_iso()
        partitions_to_process = [context] if context else self.partitions
        markers = self.stream_config.get("markers", []) or []
        reviewer = self.stream_config.get("reviewer_clause", "") or ""
        cfg_source = getattr(self, "_search_cfg", None) or self.config
        instance = cfg_source.get("search", {}).get("scope", {}).get("instance") or DEFAULT_INSTANCE

        for partition in partitions_to_process:
            org = partition["org"]
            month = partition["month"]
            query = partition["search_query"]
            api_url_base = partition["api_url_base"]

            for window_query in self._iter_processable_queries(query, api_url_base, org):
                yield from self._process_window(
                    window_query,
                    api_url_base,
                    instance,
                    org,
                    month,
                    now,
                    markers,
                    reviewer,
                )
