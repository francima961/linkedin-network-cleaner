"""
Sent invitations analyzer — matches pending invites against target accounts,
target prospects, and optionally AI-scores them for audience fit.

Reuses matching logic patterns from analyzer.py (account name normalization,
prospect ID lookup, column auto-detection).
"""

import logging
from pathlib import Path

import pandas as pd

from . import config
from .analyzer import (
    _normalize_company_name,
    _detect_column,
    _ACCOUNT_NAME_CANDIDATES,
    _PROSPECT_ID_CANDIDATES,
    _PROSPECT_ICP_CANDIDATES,
)
from .ai_scorer import TwoTierScorer

logger = logging.getLogger(__name__)


class InviteAnalyzer:
    """Analyze sent invitations against target accounts and prospects."""

    def __init__(self, assets_dir=None, analysis_dir=None):
        self.assets_dir = Path(assets_dir or config.ASSETS_DIR)
        self.analysis_dir = Path(analysis_dir or config.ANALYSIS_DIR)

    def analyze(self, invites_data, account_name_col=None, prospect_id_col=None):
        """
        Match sent invitations against target accounts and prospects.

        Args:
            invites_data: list of invite dicts from extract-sent-invitations
            account_name_col: override for account name column detection
            prospect_id_col: override for prospect ID column detection

        Returns:
            DataFrame with added columns: is_target_account, target_account_name,
            is_target_prospect, prospect_icp_tag
        """
        if not invites_data:
            logger.warning("No invites data to analyze")
            return pd.DataFrame()

        df = pd.DataFrame(invites_data)
        df["linkedin_profile_id"] = pd.to_numeric(
            df["linkedin_profile_id"], errors="coerce"
        ).astype("Int64")

        logger.info("Analyzing %d sent invitations", len(df))

        df = self._match_accounts(df, account_name_col)
        df = self._match_prospects(df, prospect_id_col)

        matched_accounts = df["is_target_account"].sum() if "is_target_account" in df.columns else 0
        matched_prospects = df["is_target_prospect"].sum() if "is_target_prospect" in df.columns else 0
        logger.info("Analysis complete: %d target accounts, %d target prospects",
                     matched_accounts, matched_prospects)

        return df

    def ai_score(self, invites_df, api_key=None, brand_strategy_path=None,
                 persona_path=None):
        """
        AI-score sent invitations for audience fit using TwoTierScorer.

        Args:
            invites_df: DataFrame of invites (from analyze())
            api_key: Anthropic API key (defaults to config)
            brand_strategy_path: path to brand strategy markdown
            persona_path: path to persona/ICP markdown

        Returns:
            DataFrame with added columns: ai_audience_fit, ai_icp_tag, ai_reasoning
        """
        api_key = api_key or config.ANTHROPIC_API_KEY

        if brand_strategy_path is None or persona_path is None:
            from .config import find_asset_files
            found_brand, found_persona = find_asset_files(self.assets_dir)
            brand_strategy_path = brand_strategy_path or found_brand
            persona_path = persona_path or found_persona

        if brand_strategy_path is None:
            raise FileNotFoundError(
                f"No brand strategy file found in {self.assets_dir}. "
                f"Place a .md file with 'brand' in its name there."
            )
        if persona_path is None:
            raise FileNotFoundError(
                f"No ICP/persona file found in {self.assets_dir}. "
                f"Place a .md file with 'persona' or 'icp' in its name there."
            )

        scorer = TwoTierScorer(
            api_key=api_key,
            brand_strategy_path=brand_strategy_path,
            persona_path=persona_path,
        )

        # Invites have limited data — no enrichment available
        scored_df = scorer.score_network(
            invites_df,
            enrichment_data=[],
            haiku_batch_size=20,
            sonnet_batch_size=20,
            delay=1.0,
            analysis_dir=self.analysis_dir,
        )

        return scored_df

    # ── Account Matching ──────────────────────────────────────────────────

    def _match_accounts(self, df, account_name_col=None):
        """Match invite job_title company against target accounts."""
        accounts_dir = self.assets_dir / "Accounts"
        if not accounts_dir.exists():
            logger.warning("Accounts directory not found — skipping account matching")
            df["is_target_account"] = False
            df["target_account_name"] = ""
            return df

        csv_files = list(accounts_dir.glob("*.csv"))
        if not csv_files:
            df["is_target_account"] = False
            df["target_account_name"] = ""
            return df

        # Load all account CSVs
        all_accounts = []
        for csv_file in csv_files:
            try:
                adf = pd.read_csv(csv_file, encoding="utf-8", low_memory=False)
                all_accounts.append(adf)
            except Exception:
                logger.exception("Error loading account CSV: %s", csv_file.name)

        if not all_accounts:
            df["is_target_account"] = False
            df["target_account_name"] = ""
            return df

        accounts_df = pd.concat(all_accounts, ignore_index=True)
        name_col = account_name_col or _detect_column(
            accounts_df, _ACCOUNT_NAME_CANDIDATES, "account name"
        )

        # Build normalized name → original name lookup
        account_lookup = {}
        for name in accounts_df[name_col].dropna().unique():
            normalized = _normalize_company_name(str(name))
            account_lookup[normalized] = str(name)

        # Invites have job_title like "CEO at Example Inc" — parse company from it
        def _extract_and_match(job_title):
            if pd.isna(job_title) or not job_title:
                return False, ""
            company = _parse_company_from_title(str(job_title))
            if not company:
                return False, ""
            normalized = _normalize_company_name(company)
            if normalized in account_lookup:
                return True, account_lookup[normalized]
            return False, ""

        matches = df["job_title"].apply(_extract_and_match)
        df["is_target_account"] = matches.apply(lambda x: x[0])
        df["target_account_name"] = matches.apply(lambda x: x[1])

        logger.info("Account matching: %d invites at target accounts",
                     df["is_target_account"].sum())
        return df

    # ── Prospect Matching ─────────────────────────────────────────────────

    def _match_prospects(self, df, prospect_id_col=None):
        """Match invites by linkedin_profile_id against target prospect lists."""
        prospects_dir = self.assets_dir / "Prospects"
        if not prospects_dir.exists():
            logger.warning("Prospects directory not found — skipping prospect matching")
            df["is_target_prospect"] = False
            df["prospect_icp_tag"] = ""
            return df

        csv_files = list(prospects_dir.glob("*.csv"))
        if not csv_files:
            df["is_target_prospect"] = False
            df["prospect_icp_tag"] = ""
            return df

        all_prospects = []
        for csv_file in csv_files:
            try:
                pdf = pd.read_csv(csv_file, encoding="utf-8", low_memory=False)
                all_prospects.append(pdf)
            except Exception:
                logger.exception("Error loading prospect CSV: %s", csv_file.name)

        if not all_prospects:
            df["is_target_prospect"] = False
            df["prospect_icp_tag"] = ""
            return df

        prospects_df = pd.concat(all_prospects, ignore_index=True)

        # Detect ID column
        id_col = prospect_id_col or _detect_column(
            prospects_df, _PROSPECT_ID_CANDIDATES, "prospect LinkedIn ID"
        )

        prospects_df["_pid"] = pd.to_numeric(
            prospects_df[id_col], errors="coerce"
        ).astype("Int64")

        # Detect ICP tag column
        icp_col = None
        for candidate in _PROSPECT_ICP_CANDIDATES:
            matches_found = [
                c for c in prospects_df.columns
                if c.lower().strip() == candidate.lower().strip()
            ]
            if matches_found:
                icp_col = matches_found[0]
                break

        # Build lookup: pid → icp_tag
        prospect_lookup = {}
        for _, row in prospects_df.iterrows():
            pid = row.get("_pid")
            if pd.isna(pid):
                continue
            icp_tag = ""
            if icp_col:
                tag_val = row.get(icp_col)
                icp_tag = str(tag_val) if not pd.isna(tag_val) else ""
            prospect_lookup[int(pid)] = icp_tag

        def _match(pid):
            if pd.isna(pid):
                return False, ""
            pid_int = int(pid)
            if pid_int in prospect_lookup:
                return True, prospect_lookup[pid_int]
            return False, ""

        matches = df["linkedin_profile_id"].apply(_match)
        df["is_target_prospect"] = matches.apply(lambda x: x[0])
        df["prospect_icp_tag"] = matches.apply(lambda x: x[1])

        logger.info("Prospect matching: %d invites are target prospects",
                     df["is_target_prospect"].sum())
        return df


def _parse_company_from_title(job_title):
    """
    Extract company name from a job_title string like "CEO at Example Inc".
    Common patterns: "Title at Company", "Title @ Company", "Title, Company".
    Returns company string or empty string.
    """
    if not job_title:
        return ""

    # "Title at Company" or "Title @ Company"
    for separator in (" at ", " @ "):
        if separator in job_title.lower():
            idx = job_title.lower().index(separator)
            return job_title[idx + len(separator):].strip()

    # "Title, Company" (less reliable but common in LinkedIn)
    if ", " in job_title:
        parts = job_title.split(", ", 1)
        if len(parts) == 2 and len(parts[1]) > 2:
            return parts[1].strip()

    return ""
