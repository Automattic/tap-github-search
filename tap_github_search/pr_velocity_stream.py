"""PR velocity stream for GitHub search results."""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any, ClassVar, Iterable

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_github_search.search_count_streams import (
    ConfigurableSearchCountStream,
    NODES_THRESHOLD,
    SearchCountStreamBase,
)

DEFAULT_INSTANCE = "github_com"
CLOSED_CAPTURE_RE = re.compile(r"closed:(\d{4}-\d{2}-\d{2})\.\.(\d{4}-\d{2}-\d{2})")


def _hours_between(created: str | None, closed: str | None) -> float | None:
    if not created or not closed:
        return None
    created_at = datetime.fromisoformat(created.replace("Z", "+00:00"))
    closed_at = datetime.fromisoformat(closed.replace("Z", "+00:00"))
    return round((closed_at - created_at).total_seconds() / 3600.0, 4)


def _body_contains_marker(body_text: str | None, marker: str) -> bool:
    literal = marker.strip().strip("'\"")
    return bool(literal and literal.casefold() in (body_text or "").casefold())


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
        while True:
            resp = self._make_graphql_request(self.GRAPHQL_PR_VELOCITY, {"q": query, "after": after}, api_url_base)
            search = resp.json()["data"]["search"]
            for node in search["nodes"]:
                if node and node.get("number") is not None and node.get("repository"):
                    yield node
            if not search["pageInfo"]["hasNextPage"]:
                break
            after = search["pageInfo"]["endCursor"]

    def _collect_pr_ids(self, query: str, api_url_base: str) -> set[str]:
        ids: set[str] = set()
        after = None
        while True:
            resp = self._make_graphql_request(self.GRAPHQL_PR_IDS, {"q": query, "after": after}, api_url_base)
            search = resp.json()["data"]["search"]
            for node in search["nodes"]:
                if node and node.get("number") is not None and node.get("repository"):
                    ids.add(f"{node['repository']['nameWithOwner']}#{node['number']}")
            if not search["pageInfo"]["hasNextPage"]:
                break
            after = search["pageInfo"]["endCursor"]
        return ids

    def _iter_day_queries(self, query: str):
        match = CLOSED_CAPTURE_RE.search(query)
        if not match:
            yield query
            return
        current = datetime.strptime(match.group(1), "%Y-%m-%d")
        end = datetime.strptime(match.group(2), "%Y-%m-%d")
        while current <= end:
            day = current.strftime("%Y-%m-%d")
            yield query.replace(match.group(0), f"closed:{day}..{day}")
            current += timedelta(days=1)

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
        now = datetime.utcnow().isoformat() + "Z"
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

            month_count = self._search_aggregate_count(query, api_url_base)
            if month_count == 0:
                continue
            if month_count <= NODES_THRESHOLD:
                yield from self._process_window(query, api_url_base, instance, org, month, now, markers, reviewer)
            else:
                for day_query in self._iter_day_queries(query):
                    day_count = self._search_aggregate_count(day_query, api_url_base)
                    if day_count > NODES_THRESHOLD:
                        raise RuntimeError(
                            f"pr_velocity day window exceeds GitHub search cap "
                            f"({day_count} > {NODES_THRESHOLD}): {day_query}"
                        )
                    yield from self._process_window(day_query, api_url_base, instance, org, month, now, markers, reviewer)
