"""Tests for the DecisionEngine priority cascade."""

from linkedin_network_cleaner.core.decision_engine import DecisionEngine


class TestDecideConnections:
    """Test the 8-step priority cascade for connection decisions."""

    def setup_method(self):
        self.engine = DecisionEngine(ai_threshold=50)

    def test_active_dms_always_keeps(self, sample_master_df):
        result = self.engine.decide_connections(sample_master_df)
        row = result[result["full_name"] == "Active DM Person"].iloc[0]
        assert row["decision"] == "keep"
        assert "dm" in row["decision_reason"].lower() or "active" in row["decision_reason"].lower()

    def test_customer_keeps(self, sample_master_df):
        result = self.engine.decide_connections(sample_master_df)
        customer_row = result[result["full_name"] == "Customer Employee"].iloc[0]
        assert customer_row["decision"] == "keep"
        assert "customer" in customer_row["decision_reason"].lower()

    def test_former_customer_keeps(self, sample_master_df):
        result = self.engine.decide_connections(sample_master_df)
        row = result[result["full_name"] == "Former Customer"].iloc[0]
        assert row["decision"] == "keep"

    def test_target_account_keeps(self, sample_master_df):
        result = self.engine.decide_connections(sample_master_df)
        row = result[result["full_name"] == "Target Account Person"].iloc[0]
        assert row["decision"] == "keep"
        assert "Acme" in row["decision_reason"]

    def test_engagement_keeps(self, sample_master_df):
        result = self.engine.decide_connections(sample_master_df)
        row = result[result["full_name"] == "Engaged Follower"].iloc[0]
        assert row["decision"] == "keep"
        assert "liked" in row["decision_reason"].lower() or "engag" in row["decision_reason"].lower()

    def test_high_ai_score_keeps(self, sample_master_df):
        result = self.engine.decide_connections(sample_master_df)
        row = result[result["full_name"] == "High AI Score"].iloc[0]
        assert row["decision"] == "keep"
        assert "75" in row["decision_reason"] or "AI" in row["decision_reason"]

    def test_threshold_boundary_removes(self, sample_master_df):
        """Score of 49 with threshold 50 should be removed."""
        result = self.engine.decide_connections(sample_master_df)
        row = result[result["full_name"] == "Threshold Boundary"].iloc[0]
        assert row["decision"] == "remove"

    def test_threshold_boundary_at_50_keeps(self, sample_master_df):
        """Score of exactly 50 should be kept."""
        sample_master_df.loc[
            sample_master_df["full_name"] == "Threshold Boundary", "ai_audience_fit"
        ] = 50
        result = self.engine.decide_connections(sample_master_df)
        row = result[result["full_name"] == "Threshold Boundary"].iloc[0]
        assert row["decision"] == "keep"

    def test_two_way_messages_triggers_review(self, sample_master_df):
        """Profile with two-way messages should be 'review', not auto-remove."""
        result = self.engine.decide_connections(sample_master_df)
        row = result[result["full_name"] == "Has Conversation"].iloc[0]
        assert row["decision"] == "review"
        assert "message" in row["decision_reason"].lower()

    def test_unanswered_outbound_does_not_trigger_review(self, sample_master_df):
        """Single unanswered outbound message should NOT block removal."""
        result = self.engine.decide_connections(sample_master_df)
        row = result[result["full_name"] == "Unanswered Outbound"].iloc[0]
        assert row["decision"] == "remove"

    def test_default_removes(self, sample_master_df):
        result = self.engine.decide_connections(sample_master_df)
        row = result[result["full_name"] == "Nobody Special"].iloc[0]
        assert row["decision"] == "remove"

    def test_empty_dataframe(self):
        import pandas as pd
        result = self.engine.decide_connections(pd.DataFrame())
        assert len(result) == 0

    def test_custom_threshold(self, sample_master_df):
        """Different AI threshold should change decisions."""
        engine_strict = DecisionEngine(ai_threshold=80)
        result = engine_strict.decide_connections(sample_master_df)
        # Score 75 should now be removed with threshold 80
        row = result[result["full_name"] == "High AI Score"].iloc[0]
        assert row["decision"] == "remove"

    def test_priority_order_customer_beats_low_ai(self, sample_master_df):
        """Customer with low AI score should still be kept (priority 2 > priority 6)."""
        result = self.engine.decide_connections(sample_master_df)
        row = result[result["full_name"] == "Customer Employee"].iloc[0]
        assert row["decision"] == "keep"
        # AI score is 20, which is below threshold, but customer status overrides
