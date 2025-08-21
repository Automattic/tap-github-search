from __future__ import annotations

import pytest
from unittest.mock import Mock, patch
from datetime import date, datetime

from tap_github.search_count_streams import (
    ConfigurableSearchCountStream,
    create_configurable_streams,
    validate_stream_config,
)
from tap_github.tap import TapGitHub


class TestConfigurableSearchStreams:
    """Test the new configurable search streams functionality."""

    def test_configurable_stream_creation(self):
        """Test that configurable streams are created correctly from config."""
        config = {
            "search_streams": [
                {
                    "name": "security_issues",
                    "query_template": "org:{org} type:issue label:security created:{start}..{end}",
                    "count_field": "security_count"
                },
                {
                    "name": "bug_issues", 
                    "query_template": "org:{org} is:issue is:open label:bug created:{start}..{end}",
                    "count_field": "bug_count"
                }
            ],
            "search_scope": {
                "instances": [
                    {
                        "api_url_base": "https://api.github.com",
                        "instance": "github.com",
                        "streams": ["security_issues", "bug_issues"],
                        "org": ["WordPress"],
                        "repo_breakdown": False
                    }
                ]
            },
            "backfill_start_month": "2025-01",
            "backfill_end_month": "2025-01",
        }
        
        tap = TapGitHub(config=config)
        streams = create_configurable_streams(tap)
        
        assert len(streams) == 2
        stream_names = [stream.name for stream in streams]
        assert "security_issues_search_counts" in stream_names
        assert "bug_issues_search_counts" in stream_names

    def test_query_template_substitution(self):
        """Test that query templates are correctly substituted."""
        stream_config = {
            "name": "test",
            "query_template": "org:{org} type:issue label:test created:{start}..{end}",
            "count_field": "test_count"
        }
        
        mock_tap = Mock()
        mock_tap.config = {}
        stream = ConfigurableSearchCountStream(stream_config, mock_tap)
        
        query = stream._build_search_query("WordPress", "2025-01-01", "2025-01-31", "test")
        expected = "org:WordPress type:issue label:test created:2025-01-01..2025-01-31"
        assert query == expected

    def test_config_validation(self):
        """Test validation of stream configuration."""
        # Valid config
        valid_config = {
            "name": "test",
            "query_template": "org:{org} type:issue created:{start}..{end}",
            "count_field": "test_count"
        }
        errors = validate_stream_config(valid_config)
        assert errors == []
        
        # Invalid config - missing required fields
        invalid_config = {"name": "test"}
        errors = validate_stream_config(invalid_config)
        assert len(errors) >= 2
        assert any("query_template" in error for error in errors)
        assert any("count_field" in error for error in errors)


class TestMultiInstanceSearchScope:
    """Test the new multi-instance search scope functionality."""

    def test_multi_instance_partition_generation(self):
        """Test that partitions are generated for multiple instances."""
        config = {
            "search_streams": [
                {
                    "name": "issues",
                    "query_template": "org:{org} type:issue is:open created:{start}..{end}",
                    "count_field": "issue_count"
                }
            ],
            "search_scope": {
                "instances": [
                    {
                        "api_url_base": "https://api.github.com",
                        "instance": "github.com",
                        "streams": ["issues"],
                        "org": ["WordPress"],
                        "repo_breakdown": False
                    },
                    {
                        "api_url_base": "https://github.example.com/api/v3",
                        "instance": "example",
                        "streams": ["issues"],
                        "org": ["ExampleOrg"],
                        "repo_breakdown": True
                    }
                ]
            },
            "backfill_start_month": "2025-01",
            "backfill_end_month": "2025-01",
        }
        
        tap = TapGitHub(config=config)
        streams = create_configurable_streams(tap)
        stream = streams[0]
        partitions = stream.partitions
        
        # Should have partitions for both instances
        github_partitions = [p for p in partitions if p["source"] == "github.com"]
        example_partitions = [p for p in partitions if p["source"] == "example"]
        
        assert len(github_partitions) > 0
        assert len(example_partitions) > 0
        
        # Check API base URLs
        assert all(p["api_url_base"] == "https://api.github.com" for p in github_partitions)
        assert all(p["api_url_base"] == "https://github.example.com/api/v3" for p in example_partitions)

    def test_instance_stream_filtering(self):
        """Test that instances only process their supported streams."""
        config = {
            "search_streams": [
                {
                    "name": "issues",
                    "query_template": "org:{org} type:issue is:open created:{start}..{end}",
                    "count_field": "issue_count"
                }
            ],
            "search_scope": {
                "instances": [
                    {
                        "api_url_base": "https://api.github.com",
                        "instance": "github.com",
                        "streams": ["issues"],  # Supports this stream
                        "org": ["WordPress"],
                        "repo_breakdown": False
                    },
                    {
                        "api_url_base": "https://github.example.com/api/v3",
                        "instance": "example",
                        "streams": ["bugs"],  # Doesn't support "issues" stream
                        "org": ["ExampleOrg"],
                        "repo_breakdown": False
                    }
                ]
            },
            "backfill_start_month": "2025-01",
            "backfill_end_month": "2025-01",
        }
        
        tap = TapGitHub(config=config)
        streams = create_configurable_streams(tap)
        stream = streams[0]
        partitions = stream.partitions
        
        # Only github.com should have partitions
        github_partitions = [p for p in partitions if p["source"] == "github.com"]
        example_partitions = [p for p in partitions if p["source"] == "example"]
        
        assert len(github_partitions) > 0
        assert len(example_partitions) == 0


class TestIncrementalReplication:
    """Test the new incremental replication functionality for search streams."""

    def test_month_range_generation(self):
        """Test that month ranges are generated correctly for backfill."""
        config = {
            "search_streams": [
                {
                    "name": "issues",
                    "query_template": "org:{org} type:issue is:open created:{start}..{end}",
                    "count_field": "issue_count"
                }
            ],
            "search_scope": {
                "instances": [
                    {
                        "api_url_base": "https://api.github.com",
                        "instance": "github.com",
                        "streams": ["issues"],
                        "org": ["WordPress"],
                        "repo_breakdown": False
                    }
                ]
            },
            "backfill_start_month": "2025-01",
            "backfill_end_month": "2025-02",
        }
        
        tap = TapGitHub(config=config)
        streams = create_configurable_streams(tap)
        stream = streams[0]
        months = stream._get_months_to_process()
        
        assert months == ["2025-01", "2025-02"]

    def test_state_bookmark_filtering(self):
        """Test that partitions are filtered based on state bookmarks."""
        config = {
            "search_streams": [
                {
                    "name": "issues",
                    "query_template": "org:{org} type:issue is:open created:{start}..{end}",
                    "count_field": "issue_count"
                }
            ],
            "search_scope": {
                "instances": [
                    {
                        "api_url_base": "https://api.github.com",
                        "instance": "github.com",
                        "streams": ["issues"],
                        "org": ["WordPress"],
                        "repo_breakdown": False
                    }
                ]
            },
            "backfill_start_month": "2025-01",
            "backfill_end_month": "2025-02",
        }
        
        tap = TapGitHub(config=config)
        
        # Test the bookmark logic directly on the stream
        streams = create_configurable_streams(tap)
        stream = streams[0]
        
        # Test with no bookmark (should include all months)
        assert stream._should_include_month("2025-01", None) == True
        assert stream._should_include_month("2025-02", None) == True
        
        # Test with bookmark (should only include months after bookmark)
        bookmark_date = date(2025, 1, 1)
        assert stream._should_include_month("2025-01", bookmark_date) == False
        assert stream._should_include_month("2025-02", bookmark_date) == True


class TestRepositoryLevelSearches:
    """Test the new repository-level search functionality."""

    def test_repo_level_partition_generation(self):
        """Test that partitions are generated for repository-level searches."""
        config = {
            "search_streams": [
                {
                    "name": "issues",
                    "query_template": "org:{org} type:issue is:open created:{start}..{end}",
                    "count_field": "issue_count"
                }
            ],
            "search_scope": {
                "instances": [
                    {
                        "api_url_base": "https://api.github.com",
                        "instance": "github.com",
                        "streams": ["issues"],
                        "org": ["WordPress"],
                        "repo_level": ["WordPress/gutenberg", "ExampleOrg/example-repo"],
                        "repo_breakdown": False
                    }
                ]
            },
            "backfill_start_month": "2025-01",
            "backfill_end_month": "2025-01",
        }
        
        tap = TapGitHub(config=config)
        streams = create_configurable_streams(tap)
        stream = streams[0]
        partitions = stream.partitions
        
        # Should have partitions for both repositories
        gutenberg_partitions = [p for p in partitions if "gutenberg" in p["search_name"]]
        example_partitions = [p for p in partitions if "example-repo" in p["search_name"]]
        
        assert len(gutenberg_partitions) > 0
        assert len(example_partitions) > 0
        
        # Check that org is extracted correctly from repo
        for partition in gutenberg_partitions:
            assert partition["org"] == "WordPress"
        for partition in example_partitions:
            assert partition["org"] == "ExampleOrg"

    def test_repo_level_query_generation(self):
        """Test query generation for repository-level searches."""
        stream_config = {
            "name": "issues",
            "query_template": "org:{org} type:issue is:open created:{start}..{end}",
            "count_field": "issue_count"
        }
        
        mock_tap = Mock()
        mock_tap.config = {}
        stream = ConfigurableSearchCountStream(stream_config, mock_tap)
        
        # Test the built-in repo search query method
        query = stream._build_repo_search_query("WordPress/gutenberg", "2025-01-01", "2025-01-31", "issues")
        expected = "repo:WordPress/gutenberg type:issue is:open created:2025-01-01..2025-01-31"
        assert query == expected


class TestWorkflow:
    """Workflow tests for the complete search streams functionality."""

    def test_full_search_streams_workflow(self):
        """Test the complete workflow from configuration to stream creation."""
        config = {
            "search_streams": [
                {
                    "name": "security_issues",
                    "query_template": "org:{org} type:issue label:security created:{start}..{end}",
                    "count_field": "security_count"
                },
                {
                    "name": "bug_issues",
                    "query_template": "org:{org} is:issue is:open label:bug created:{start}..{end}",
                    "count_field": "bug_count"
                }
            ],
            "search_scope": {
                "instances": [
                    {
                        "api_url_base": "https://api.github.com",
                        "instance": "github.com",
                        "streams": ["security_issues", "bug_issues"],
                        "org": ["WordPress"],
                        "repo_level": ["WordPress/gutenberg"],
                        "repo_breakdown": False
                    }
                ]
            },
            "backfill_start_month": "2025-01",
            "backfill_end_month": "2025-01",
        }
        
        tap = TapGitHub(config=config)
        streams = create_configurable_streams(tap)
        
        assert len(streams) == 2
        
        # Test security issues stream
        security_stream = streams[0]
        assert security_stream.name == "security_issues_search_counts"
        security_partitions = security_stream.partitions
        assert len(security_partitions) > 0
        
        # Test bug issues stream
        bug_stream = streams[1]
        assert bug_stream.name == "bug_issues_search_counts"
        bug_partitions = bug_stream.partitions
        assert len(bug_partitions) > 0
        
        # Verify partition structure
        for partition in security_partitions + bug_partitions:
            assert "search_name" in partition
            assert "search_query" in partition
            assert "source" in partition
            assert "org" in partition
            assert "month" in partition
            assert "api_url_base" in partition
            assert "repo_breakdown" in partition

    def test_error_handling_invalid_config(self):
        """Test graceful handling of invalid configurations."""
        # Test validation function directly since tap validates at init
        valid_config = {
            "name": "valid",
            "query_template": "org:{org} type:issue created:{start}..{end}",
            "count_field": "valid_count"
        }
        invalid_config = {
            "name": "invalid"  # Missing required fields
        }
        
        # Valid config should pass validation
        errors = validate_stream_config(valid_config)
        assert errors == []
        
        # Invalid config should fail validation
        errors = validate_stream_config(invalid_config)
        assert len(errors) >= 2
        assert any("query_template" in error for error in errors)
        assert any("count_field" in error for error in errors)
