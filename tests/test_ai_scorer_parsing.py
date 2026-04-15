"""Tests for AI scorer response parsing."""

import json
from unittest.mock import MagicMock

from linkedin_network_cleaner.core.ai_scorer import TwoTierScorer


class TestParseHaikuResponse:
    """Test _parse_haiku_response with various response formats."""

    def setup_method(self):
        """Create a scorer instance with mock files."""
        # We need to test parsing without real API keys
        # Create the instance carefully or test the method directly
        pass

    def _make_response(self, text):
        """Create a mock Anthropic response."""
        mock = MagicMock()
        mock.content = [MagicMock(text=text)]
        return mock

    def _get_scorer_for_parsing(self, tmp_path):
        """Create a TwoTierScorer with minimal mock files for testing parsing."""
        brand = tmp_path / "brand.md"
        brand.write_text("Test brand strategy")
        persona = tmp_path / "persona.md"
        persona.write_text("Test persona definition")
        # Mock the anthropic client to avoid needing a real key
        scorer = object.__new__(TwoTierScorer)
        scorer.client = MagicMock()
        scorer.haiku_model = "test"
        scorer.sonnet_model = "test"
        scorer._haiku_system = "test"
        scorer._sonnet_system = "test"
        return scorer

    def test_valid_json_array(self, tmp_path):
        scorer = self._get_scorer_for_parsing(tmp_path)
        response = self._make_response('[{"id": 123, "d": "KEEP", "r": "good fit"}]')
        batch = [{"id": 123}]
        results = scorer._parse_haiku_response(response, batch)
        assert len(results) == 1
        assert results[0]["id"] == 123
        assert results[0]["d"] == "KEEP"

    def test_code_block_wrapped(self, tmp_path):
        scorer = self._get_scorer_for_parsing(tmp_path)
        response = self._make_response('```json\n[{"id": 456, "d": "REMOVE", "r": "off target"}]\n```')
        batch = [{"id": 456}]
        results = scorer._parse_haiku_response(response, batch)
        assert len(results) == 1
        assert results[0]["d"] == "REMOVE"

    def test_malformed_json_falls_back_to_review(self, tmp_path):
        scorer = self._get_scorer_for_parsing(tmp_path)
        response = self._make_response('this is not json at all')
        batch = [{"id": 789}, {"id": 101}]
        results = scorer._parse_haiku_response(response, batch)
        assert len(results) == 2
        assert all(r["d"] == "REVIEW" for r in results)

    def test_invalid_decision_normalized(self, tmp_path):
        scorer = self._get_scorer_for_parsing(tmp_path)
        response = self._make_response('[{"id": 1, "d": "MAYBE", "r": "unsure"}]')
        batch = [{"id": 1}]
        results = scorer._parse_haiku_response(response, batch)
        assert results[0]["d"] == "REVIEW"  # invalid "MAYBE" should become "REVIEW"

    def test_empty_response(self, tmp_path):
        scorer = self._get_scorer_for_parsing(tmp_path)
        response = self._make_response('')
        batch = [{"id": 1}]
        results = scorer._parse_haiku_response(response, batch)
        # Should fall back to REVIEW for entire batch
        assert all(r["d"] == "REVIEW" for r in results)


class TestParseSonnetResponse:

    def _make_response(self, text):
        mock = MagicMock()
        mock.content = [MagicMock(text=text)]
        return mock

    def _get_scorer_for_parsing(self, tmp_path):
        scorer = object.__new__(TwoTierScorer)
        scorer.client = MagicMock()
        scorer.haiku_model = "test"
        scorer.sonnet_model = "test"
        scorer._haiku_system = "test"
        scorer._sonnet_system = "test"
        return scorer

    def test_valid_sonnet_response(self, tmp_path):
        scorer = self._get_scorer_for_parsing(tmp_path)
        data = [{"linkedin_profile_id": 1, "audience_fit_score": 85, "icp_tag": "DM", "reasoning": "Decision maker at SaaS"}]
        response = self._make_response(json.dumps(data))
        results = scorer._parse_sonnet_response(response)
        assert len(results) == 1
        assert results[0]["audience_fit_score"] == 85
        assert results[0]["icp_tag"] == "DM"

    def test_invalid_icp_tag_normalized(self, tmp_path):
        scorer = self._get_scorer_for_parsing(tmp_path)
        data = [{"linkedin_profile_id": 1, "audience_fit_score": 50, "icp_tag": "INVALID_TAG", "reasoning": "test"}]
        response = self._make_response(json.dumps(data))
        results = scorer._parse_sonnet_response(response)
        assert results[0]["icp_tag"] == "NONE"

    def test_malformed_json_returns_empty(self, tmp_path):
        scorer = self._get_scorer_for_parsing(tmp_path)
        response = self._make_response('not valid json')
        results = scorer._parse_sonnet_response(response)
        assert results == []

    def test_code_block_wrapped_sonnet(self, tmp_path):
        scorer = self._get_scorer_for_parsing(tmp_path)
        data = [{"linkedin_profile_id": 2, "audience_fit_score": 60, "icp_tag": "INFLUENCER", "reasoning": "tech influencer"}]
        response = self._make_response(f'```json\n{json.dumps(data)}\n```')
        results = scorer._parse_sonnet_response(response)
        assert len(results) == 1
        assert results[0]["icp_tag"] == "INFLUENCER"
