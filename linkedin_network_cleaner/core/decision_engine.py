"""
Decision engine — rules-based keep/review/remove decisions for network cleanup.
"""

import logging
from datetime import datetime, timedelta

import pandas as pd

logger = logging.getLogger(__name__)


class DecisionEngine:
    """Rules-based decision maker for network cleanup actions."""

    def __init__(self, ai_threshold=50, stale_days=21, safelist=None,
                 keep_rules=None, signal_config=None):
        """
        Args:
            ai_threshold: Minimum ai_audience_fit score to consider "valuable" (default 50)
            stale_days: Days after which an unanswered invite is "stale" (default 21)
            safelist: Set of profile URLs/handles that are NEVER removed
            keep_rules: Dict with keep_locations, keep_companies, keep_title_keywords lists
            signal_config: Dict controlling which engagement signals trigger keep:
                - keep_likers (bool, default True): people who liked your posts
                - keep_commenters (bool, default True): people who commented on your posts
                - keep_reposters (bool, default True): people who reposted your posts
                - keep_content_interactions (bool, default True): people whose content you engaged with
        """
        self.ai_threshold = ai_threshold
        self.stale_days = stale_days
        self.safelist = safelist or set()
        self.keep_rules = keep_rules or {}
        self.signal_config = signal_config or {
            "keep_likers": True,
            "keep_commenters": True,
            "keep_reposters": True,
            "keep_content_interactions": True,
        }

    # ── Invite Decisions ──────────────────────────────────────────────────

    def decide_invites(self, invites_df):
        """Apply invite cleanup rules. Adds decision + decision_reason columns."""
        df = invites_df.copy()
        if df.empty:
            df["decision"] = pd.Series(dtype="object")
            df["decision_reason"] = pd.Series(dtype="object")
            return df

        decisions = []
        reasons = []

        for _, row in df.iterrows():
            sent_date = _parse_sent_date(row.get("sent_date"))
            is_stale = False
            if sent_date:
                is_stale = (datetime.now() - sent_date) > timedelta(days=self.stale_days)

            is_target = row.get("is_target_account", False) or row.get("is_target_prospect", False)
            ai_fit = row.get("ai_audience_fit")
            is_valuable = is_target or (ai_fit is not None and not pd.isna(ai_fit) and int(ai_fit) >= self.ai_threshold)

            if is_valuable and not is_stale:
                decisions.append("keep")
                reasons.append("Valuable prospect, invite still fresh")
            elif is_valuable and is_stale:
                decisions.append("withdraw_and_tag")
                reasons.append(f"Valuable but stale ({self.stale_days}+ days) — withdraw and re-approach")
            else:
                if is_stale:
                    decisions.append("withdraw")
                    reasons.append(f"Not valuable, stale ({self.stale_days}+ days)")
                else:
                    decisions.append("withdraw")
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
            0. Safelist / custom keep rules  → keep (always)
            1. active_dms=True               → keep (active DM relationship)
            2. is_customer/former_customer   → keep
            3. is_target_account/prospect    → keep
            4. Engaged with your posts       → keep (likers/commenters/reposters, configurable)
            5. You engaged with their content→ keep (your likes/comments, configurable)
            6. shared_school/experience      → keep
            7. ai_audience_fit >= threshold  → keep
            8. Has some messages             → review (needs human)
            9. everything else               → remove

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

        # 1. Active DM relationship — always keep
        if row.get("active_dms", False):
            return "keep", "Active DM relationship"

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

        # 4. Engaged with your posts (configurable per signal type)
        engagement_parts = []
        if self.signal_config.get("keep_likers", True):
            likes = int(row.get("times_liked", 0) or 0)
            if likes > 0:
                engagement_parts.append(f"{likes} like{'s' if likes != 1 else ''}")
        if self.signal_config.get("keep_commenters", True):
            comments = int(row.get("times_commented", 0) or 0)
            if comments > 0:
                engagement_parts.append(f"{comments} comment{'s' if comments != 1 else ''}")
        if self.signal_config.get("keep_reposters", True):
            reposts = int(row.get("times_reposted", 0) or 0)
            if reposts > 0:
                engagement_parts.append(f"{reposts} repost{'s' if reposts != 1 else ''}")
        if engagement_parts:
            return "keep", f"Engaged with your posts ({', '.join(engagement_parts)})"

        # 5. You engaged with their content (configurable)
        if self.signal_config.get("keep_content_interactions", True):
            your_likes = int(row.get("i_liked_their_posts", 0) or 0)
            your_comments = int(row.get("i_commented_their_posts", 0) or 0)
            if your_likes > 0 or your_comments > 0:
                parts = []
                if your_likes > 0:
                    parts.append(f"liked {your_likes}")
                if your_comments > 0:
                    parts.append(f"commented {your_comments}")
                return "keep", f"You engaged with their content ({', '.join(parts)})"

        # 6. Shared school or experience
        if row.get("shared_school", False):
            return "keep", "Shared school"
        if row.get("shared_experience", False):
            return "keep", "Shared work experience"

        # 7. AI audience fit above threshold
        ai_fit = row.get("ai_audience_fit")
        if ai_fit is not None and not pd.isna(ai_fit):
            ai_fit = int(ai_fit)
            if ai_fit >= self.ai_threshold:
                tag = row.get("ai_icp_tag", "")
                return "keep", f"AI fit {ai_fit}/100 (ICP: {tag})"

        # 8. Has messages — needs human review (but not single unanswered outbound)
        total_messages = int(row.get("total_messages", 0) or 0)
        their_messages = int(row.get("their_messages", 0) or 0)
        if total_messages > 0 and their_messages > 0:
            return "review", f"Has {total_messages} messages ({their_messages} from them) — needs human review"

        # 9. Default — remove
        return "remove", "No engagement, not a target, no AI fit"


# ── Helpers ───────────────────────────────────────────────────────────────

def _parse_sent_date(date_val):
    """Parse sent_date string to datetime. Returns None if unparseable."""
    if date_val is None or (isinstance(date_val, float) and pd.isna(date_val)):
        return None
    date_str = str(date_val).strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%b %d, %Y"):
        try:
            return datetime.strptime(date_str[:len(fmt) + 5], fmt)
        except ValueError:
            continue
    return None
