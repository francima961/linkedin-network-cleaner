"""Tests for analyzer matching and normalization logic."""

import pandas as pd
import pytest
from pathlib import Path

# Import the module-level helpers from analyzer
from linkedin_network_cleaner.core.analyzer import (
    _normalize_company_name,
    _detect_column,
    _parse_date,
    _dates_overlap,
    _extract_handle_from_url,
    _match_customer_name,
)


class TestNormalizeCompanyName:

    def test_basic_normalization(self):
        assert _normalize_company_name("Acme Corp") == "acme"

    def test_strips_inc(self):
        assert _normalize_company_name("TechStart Inc.") == "techstart"

    def test_strips_llc(self):
        assert _normalize_company_name("DataFlow LLC") == "dataflow"

    def test_strips_ltd(self):
        assert _normalize_company_name("Global Solutions Ltd") == "global solutions"

    def test_lowercase(self):
        assert _normalize_company_name("ACME CORPORATION") == "acme"

    def test_strips_trailing_punctuation(self):
        assert _normalize_company_name("Test Company, ") == "test company"

    def test_collapses_whitespace(self):
        assert _normalize_company_name("Big   Tech   Inc") == "big tech"

    def test_empty_string(self):
        assert _normalize_company_name("") == ""


class TestDetectColumn:

    def test_exact_match(self):
        df = pd.DataFrame({"Company": [1], "Other": [2]})
        assert _detect_column(df, ["Company", "company_name"], "test") == "Company"

    def test_case_insensitive(self):
        df = pd.DataFrame({"COMPANY": [1], "Other": [2]})
        assert _detect_column(df, ["company", "Company"], "test") == "COMPANY"

    def test_first_candidate_wins(self):
        df = pd.DataFrame({"company_name": [1], "Company": [2]})
        assert _detect_column(df, ["company_name", "Company"], "test") == "company_name"

    def test_raises_on_no_match(self):
        df = pd.DataFrame({"X": [1], "Y": [2]})
        with pytest.raises(ValueError, match="Could not auto-detect"):
            _detect_column(df, ["company_name", "Company"], "test column")


class TestMatchCustomerName:

    def test_exact_match(self):
        lookup = {"acme", "techstart"}
        assert _match_customer_name("Acme Corp", lookup) is True

    def test_starts_with_match(self):
        lookup = {"agicap"}
        assert _match_customer_name("Agicap France", lookup) is True

    def test_no_match(self):
        lookup = {"acme"}
        assert _match_customer_name("Totally Different Company", lookup) is False

    def test_none_input(self):
        assert _match_customer_name(None, {"acme"}) is False

    def test_empty_string(self):
        assert _match_customer_name("", {"acme"}) is False


class TestParseDate:

    def test_year_month(self):
        assert _parse_date("2020-01") == (2020, 1)

    def test_month_year(self):
        assert _parse_date("Jan 2020") == (2020, 1)

    def test_bare_year(self):
        assert _parse_date("2020") == (2020, 1)

    def test_present(self):
        assert _parse_date("Present") == (9999, 12)

    def test_empty(self):
        assert _parse_date("") is None

    def test_none(self):
        assert _parse_date(None) is None


class TestDatesOverlap:

    def test_overlap(self):
        assert _dates_overlap((2020, 1), (2022, 6), (2021, 1), (2023, 1)) is True

    def test_no_overlap(self):
        assert _dates_overlap((2020, 1), (2020, 12), (2022, 1), (2023, 1)) is False

    def test_one_present(self):
        assert _dates_overlap((2020, 1), (9999, 12), (2022, 1), (2023, 1)) is True

    def test_none_bounds(self):
        assert _dates_overlap(None, None, None, None) is True


class TestExtractHandle:

    def test_standard_url(self):
        assert _extract_handle_from_url("https://www.linkedin.com/in/johndoe/") == "johndoe"

    def test_posts_url(self):
        assert _extract_handle_from_url("https://www.linkedin.com/posts/johndoe_some-post") == "johndoe"

    def test_empty(self):
        assert _extract_handle_from_url("") == ""

    def test_none(self):
        assert _extract_handle_from_url(None) == ""
