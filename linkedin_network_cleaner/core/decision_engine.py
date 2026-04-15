"""
Decision engine for LinkedIn network cleanup.

Applies rules to determine which invitations to withdraw and which
connections to remove. All decisions are explainable via decision_reason column.
"""

import logging
from datetime import datetime, timezone, timedelta

import pandas as pd

logger = logging.getLogger(__name__)


class DecisionEngine:
    """Rules-based decision maker for network cleanup actions."""

    def __init__(self, ai_threshold=50, stale_days=21, safelist=None, keep_rules=None):
        """
        Args:
            ai_threshold: Minimum ai_audience_fit score to consider "valuable" (default 50)
            stale_days: Days after which an unanswered invite is "stale" (default 21)
            safelist: Set of profile URLs/handles that are NEVER removed
            keep_rules: Dict with keep_locations, keep_companies, keep_title_keywords lists
        """
        self.ai_threshold = ai_threshold
        self.stale_days = stale_days
        self.safelist = safelist or set()
        self.keep_rules = keep_rules or {}

    # ── Invite Decisions ──────────────────────────────────────────────────

    def decide_invites(self, invites_df):
        """
        Apply decision rules to sent invitations.

        Decision matrix:
            valuable + fresh       → "keep"
            valuable + stale       → "withdraw_and_tag" (withdraw but tag for re-adding)
            not valuable + fresh   → "withdraw"
            not valuable + stale   → "withdraw"

        Args:
            invites_df: DataFrame from InviteAnalyzer.analyze() with columns:
                sent_date, is_target_account, is_target_prospect,
                optionally ai_audience_fit

        Returns:
            Same DataFrame with added columns:
                is_valuable, is_stale, decision, decision_reason
        """
        df = invites_df.copy()

        if df.empty:
            for col in ("is_valuable", "is_stale", "decision", "decision_reason"):
                df[col] = pd.Series(dtype="object")
            return df

        # Compute is_valuable
        is_target_account = df.get("is_target_account", pd.Series(False, index=df.index)).fillna(False)
        is_target_prospect = df.get("is_target_prospect", pd.Series(False, index=df.index)).fillna(False)

        has_ai_score = "ai_audience_fit" in df.columns and df["ai_audience_fit"].notna().any()
        if has_ai_score:
            ai_fit = pd.to_numeric(df["ai_audience_fit"], errors="coerce").fillna(0)
            ai_valuable = ai_fit >= self.ai_threshold
        else:
            ai_valuable = pd.Series(False, index=df.index)

        df["is_valuable"] = is_target_account | is_target_prospect | ai_valuable

        # Compute is_stale
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.stale_days)
        df["is_stale"] = df["sent_date"].apply(
            lambda x: _parse_sent_date(x) < cutoff if _parse_sent_date(x) else True
        )

        # Apply decision matrix
        decisions = []
        reasons = []

        for _, row in df.iterrows():
            valuable = row["is_valuable"]
            stale = row["is_stale"]

            if valuable and not stale:
                decisions.append("keep")
                reasons.append(_valuable_reason(row, self.ai_threshold))
            elif valuable and stale:
                decisions.append("withdraw_and_tag")
                reasons.append(
                    f"Stale ({self.stale_days}+ days) but valuable — "
                    + _valuable_reason(row, self.ai_threshold)
                )
            else:
                decisions.append("withdraw")
                if stale:
                    reasons.append(f"Not valuable + stale ({self.stale_days}+ days)")
                else:
                    reasons.append("Not valuable — no account/prospect/AI match")

        df["decision"] = decisions
        df["decision_reason"] = reasons

        # Summary
        counts = df["decision"].value_counts()
        logger.info("Invite decisions: %s", dict(counts))

        return df

    # ── Connection Decisions ──────────────────────────────────────────────

    def decide_connections(self, master_df):
        """
        Apply conservative priority cascade to connection removal decisions.

        Priority (first match wins):
            1. real_network=True          → keep  (always)
            2. is_customer/former_customer→ keep  (works/worked at customer company)
            3. is_target_account/prospect → keep
            4. total_engagements > 0      → keep  (engaged with content)
            5. shared_school/experience   → keep
            6. ai_audience_fit >= threshold→ keep
            7. total_messages > 0         → review (has conversation, needs human)
            8. everything else            → remove

        Args:
            master_df: network_master DataFrame from analysis pipeline

        Returns:
            Same DataFrame with added columns: decision, decision_reason
        """
        df = master_df.copy()

        if df.empty:
            df["decision"] = pd.Series(dtype="object")
            df["decision_reason"] = pd.Series(dtype="object")
            return df

        decisions = []
        reasons = []

        for _, row in df.iterrows():
            decision, reason = self._decide_single_connection(row)
            decisions.append(decision)
            reasons.append(reason)

        df["decision"] = decisions
        df["decision_reason"] = reasons

        # Summary
        counts = df["decision"].value_counts()
        logger.info("Connection decisions: %s", dict(counts))

        return df

    def _decide_single_connection(self, row):
        """Apply priority cascade to a single connection row."""
        # 0. Safelist — NEVER remove
        if self.safelist:
            pid = row.get("linkedin_profile_id")
            url = row.get("linkedin_profile_url", "")
            handle = row.get("linkedin_profile_handle", "")

            if pid is not None and str(pid) in self.safelist:
                return "keep", "Protected: safelist"
            if url and url in self.safelist:
                return "keep", "Protected: safelist"
            if handle and handle.lower() in self.safelist:
                return "keep", "Protected: safelist"

        # 0b. Custom keep rules
        if self.keep_rules:
            location = str(row.get("location", "")).lower()
            company = str(row.get("current_company", "")).lower()
            title = str(row.get("current_job_title", "")).lower()

            for loc in self.keep_rules.get("keep_locations", []):
                if loc and loc in location:
                    return "keep", f"Keep rule: location matches '{loc}'"
            for comp in self.keep_rules.get("keep_companies", []):
                if comp and comp in company:
                    return "keep", f"Keep rule: company matches '{comp}'"
            for kw in self.keep_rules.get("keep_title_keywords", []):
                if kw and kw in title:
                    return "keep", f"Keep rule: title contains '{kw}'"

        # 1. Real network — always keep
        if row.get("real_network", False):
            return "keep", "Real network (active inbox relationship)"

        # 2. Customer or former customer — always keep
        if row.get("is_customer", False):
            return "keep", "Works at a customer company"
        if row.get("is_former_customer", False):
            return "keep", "Previously at a customer company"

        # 3. Target account or prospect
        if row.get("is_target_account", False):
            name = row.get("target_account_name", "")
            return "keep", f"Target account: {name}"
        if row.get("is_target_prospect", False):
            tag = row.get("prospect_icp_tag", "")
            return "keep", f"Target prospect (ICP: {tag})" if tag else "Target prospect"

        # 4. Engaged with content
        engagements = int(row.get("total_engagements", 0) or 0)
        if engagements > 0:
            return "keep", f"Engaged with content ({engagements} interactions)"

        # 5. Shared school or experience
        if row.get("shared_school", False):
            return "keep", "Shared school"
        if row.get("shared_experience", False):
            return "keep", "Shared work experience"

        # 6. AI audience fit above threshold
        ai_fit = row.get("ai_audience_fit")
        if ai_fit is not None and not pd.isna(ai_fit):
            ai_fit = int(ai_fit)
            if ai_fit >= self.ai_threshold:
                tag = row.get("ai_icp_tag", "")
                return "keep", f"AI fit {ai_fit}/100 (ICP: {tag})"

        # 7. Has messages — needs human review (but not single unanswered outbound)
        total_messages = int(row.get("total_messages", 0) or 0)
        their_messages = int(row.get("their_messages", 0) or 0)
        if total_messages > 0 and their_messages > 0:
            return "review", f"Has {total_messages} messages ({their_messages} from them) — needs human review"

        # 8. Default — remove
        return "remove", "No engagement, not a target, no AI fit"


# ── Helpers ───────────────────────────────────────────────────────────────

def _parse_sent_date(date_val):
    """Parse sent_date string to datetime. Returns None if unparseable."""
    if date_val is None or (isinstance(date_val, float) and pd.isna(date_val)):
        return None
    if isinstance(date_val, datetime):
        if date_val.tzinfo is None:
            return date_val.replace(tzinfo=timezone.utc)
        return date_val

    date_str = str(date_val).strip()
    if not date_str:
        return None

    # Try ISO format variants
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(date_str, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue

    return None


def _valuable_reason(row, threshold):
    """Build a human-readable reason string for why an invite is valuable."""
    parts = []
    if row.get("is_target_account", False):
        name = row.get("target_account_name", "")
        parts.append(f"Target account: {name}" if name else "Target account")
    if row.get("is_target_prospect", False):
        tag = row.get("prospect_icp_tag", "")
        parts.append(f"Target prospect (ICP: {tag})" if tag else "Target prospect")
    ai_fit = row.get("ai_audience_fit")
    if ai_fit is not None and not pd.isna(ai_fit) and int(ai_fit) >= threshold:
        parts.append(f"AI fit {int(ai_fit)}/100")
    return "; ".join(parts) if parts else "Meets value criteria"
