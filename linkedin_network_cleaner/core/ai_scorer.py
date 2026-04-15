"""
Two-tier AI scoring: Haiku triage → Sonnet deep-score on ambiguous cases.

Tier 1 (Haiku): Lean prompt, minimal profile data (title + company + summary).
  Classifies: KEEP / REMOVE / REVIEW
  Batches of 50, ~$2-4 for 7,885 profiles.

Tier 2 (Sonnet): Full prompt + enrichment data, only for REVIEW cases from Tier 1.
  Full scoring: 0-100 + ICP tag + reasoning.

Usage:
    from linkedin_network_cleaner.core.ai_scorer import TwoTierScorer
    scorer = TwoTierScorer(api_key, brand_strategy_path, persona_path)
    master_df = scorer.score_network(master_df, enrichment_data)
"""

import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import anthropic

from . import config

logger = logging.getLogger(__name__)

# ── Tier 1: Haiku triage prompt (lean, ~2KB) ─────────────────────────────

HAIKU_PROMPT_TEMPLATE = """You classify LinkedIn profiles for audience fit based on the target audience described below.

## Target Audience (Summary)
{condensed}

## Classification Rules
KEEP: Clearly matches the target personas, relevant roles, or companies in the target industry
REMOVE: Clearly outside the target audience — wrong industry, irrelevant role, no connection to target market
REVIEW: Ambiguous — title sounds relevant but company is unclear, or company matches but role is unclear

## Output format
Return ONLY a JSON array. Each object:
{{"id": <linkedin_profile_id>, "d": "KEEP"|"REMOVE"|"REVIEW", "r": "<5 words max>"}}

Be decisive. Minimize REVIEW — only use it when you genuinely cannot tell."""


def _build_haiku_system_prompt(brand_content, persona_content):
    """Build a condensed Haiku triage prompt from user's brand strategy and persona files."""
    # Truncate each to ~750 chars to keep Haiku prompt lean (~1500 chars total context)
    brand_summary = brand_content[:750].rsplit(' ', 1)[0] + "..." if len(brand_content) > 750 else brand_content
    persona_summary = persona_content[:750].rsplit(' ', 1)[0] + "..." if len(persona_content) > 750 else persona_content
    condensed = f"### Brand & Market\n{brand_summary}\n\n### Target Personas\n{persona_summary}"
    return HAIKU_PROMPT_TEMPLATE.format(condensed=condensed)

HAIKU_USER_TEMPLATE = "Classify these {count} profiles:\n{profiles}"

# ── Tier 2: Sonnet deep-score (reuses existing full prompt) ──────────────

SONNET_USER_TEMPLATE = (
    "Score these {count} LinkedIn profiles for audience fit. "
    "For each profile, return a JSON object with: "
    "linkedin_profile_id (int), audience_fit_score (0-100), "
    "icp_tag (one of: DM, C_LEVEL, CHAMPION, INFLUENCER, INVESTOR, MKTG_PARTNER, NONE), "
    "reasoning (1 sentence explaining the score).\n\n"
    "Profiles:\n{profiles_json}"
)


class TwoTierScorer:
    """Two-tier AI scoring: Haiku triage then Sonnet deep-score."""

    def __init__(self, api_key, brand_strategy_path, persona_path,
                 haiku_model="claude-haiku-4-5-20251001",
                 sonnet_model="claude-sonnet-4-6"):
        self.client = anthropic.Anthropic(api_key=api_key)
        self.haiku_model = haiku_model
        self.sonnet_model = sonnet_model

        # Load full prompts for Sonnet tier
        brand_path = Path(brand_strategy_path)
        persona_path = Path(persona_path)

        if not brand_path.exists():
            raise FileNotFoundError(f"Brand strategy not found: {brand_path}")
        if not persona_path.exists():
            raise FileNotFoundError(f"Persona file not found: {persona_path}")

        brand_content = brand_path.read_text(encoding="utf-8")
        persona_content = persona_path.read_text(encoding="utf-8")

        # Haiku system prompt — built dynamically from user's brand/persona files
        self._haiku_system = _build_haiku_system_prompt(brand_content, persona_content)

        # Sonnet system prompt — full version with inclusive scoring rules
        SONNET_SYSTEM_TEMPLATE = (
            "You are an audience fit scorer for a LinkedIn professional network.\n\n"
            "## Brand Strategy\n{brand_strategy_content}\n\n"
            "## Target Personas (ICP)\n{persona_content}\n\n"
            "## Instructions\n"
            "Score each profile for audience fit based on the brand strategy and ICP above.\n"
            "Return ONLY a JSON array with one object per profile.\n"
            "Each object must have exactly these fields:\n"
            "- linkedin_profile_id: the profile ID (int)\n"
            "- audience_fit_score: 0-100 where 100 = perfect fit\n"
            "- icp_tag: one of DM, C_LEVEL, CHAMPION, INFLUENCER, INVESTOR, MKTG_PARTNER, NONE\n"
            "- reasoning: one sentence explaining the score\n\n"
            "Scoring guidelines — INCLUSIVE BY DEFAULT (false positives are cheap to discard; missed valuable contacts are costly):\n"
            "- 80-100: Core ICP match — decision makers, champions, or influencers at SaaS/AI companies; GTM engineers; RevOps/CROs; technical founders building products\n"
            "- 65-79: Strong network value — C-levels (CMO, CTO, CEO, CRO); founders/co-founders (any stage); investors/VCs/angels/PE/M&A/family offices; growth practitioners; partnership/alliance roles\n"
            "- 50-64: Adjacent value — relevant industry but non-core role; SaaS/AI adjacent; marketing directors at tech; business development at SaaS; community peers\n"
            "- 30-49: Low fit — generic roles at non-SaaS/non-tech companies; no connection to GTM, data, product, or growth\n"
            "- 0-29: No fit — completely irrelevant industry/role (talent acquisition at nuclear company, financial analyst at banking, CS specialist at non-tech), inactive/career break with no relevant history\n\n"
            "Key role signals — score HIGH (65+) regardless of company:\n"
            "- Founding roles (founder, co-founder, founding team member, founding engineer, founding designer, founding BDR/SDR)\n"
            "- C-suite (CEO, CTO, CMO, CRO, CPO, CFO)\n"
            "- Senior GTM leadership: Head of Growth, Head of Sales, VP Sales, VP Growth, VP Marketing, Director of Operations, Director of Sales, Director of Growth\n"
            "- GTM engineering, growth engineering, RevOps, revenue operations\n"
            "- Investors, VCs, angels, board members, advisors, M&A, PE, family office\n"
            "- Product managers/engineers at SaaS or AI companies\n\n"
            'General rule: "Head of" and above in any GTM function (marketing, sales, growth, partnerships, operations) = 65+ by default.'
        )
        self._sonnet_system = SONNET_SYSTEM_TEMPLATE.format(
            brand_strategy_content=brand_content,
            persona_content=persona_content,
        )

        logger.info("TwoTierScorer initialized (haiku=%s, sonnet=%s)",
                     haiku_model, sonnet_model)

    def score_network(self, master_df, enrichment_data,
                      haiku_batch_size=50, sonnet_batch_size=20,
                      delay=1.0, analysis_dir=None,
                      rescore_haiku_keeps=False, progress_callback=None):
        """
        Two-tier scoring on connections that need evaluation.

        If rescore_haiku_keeps=True, skips the normal flow and sends all
        HAIKU_KEEP profiles through Sonnet deep scoring to replace their
        flat score of 60 with real 0-100 scores + ICP tags.
        Uses a dedicated checkpoint (sonnet_haiku_keep) for safe resume.

        Returns master_df with columns:
            ai_audience_fit, ai_icp_tag, ai_reasoning, ai_decision (KEEP/REMOVE/REVIEW)
        """
        if analysis_dir is None:
            analysis_dir = config.ANALYSIS_DIR
        analysis_dir = Path(analysis_dir)
        analysis_dir.mkdir(parents=True, exist_ok=True)

        # Build enrichment lookup
        enrichment_lookup = {}
        if enrichment_data:
            for entry in enrichment_data:
                pid = entry.get("linkedin_profile_id")
                if pid is not None:
                    enrichment_lookup[pid] = entry

        # ── Rescore mode: send HAIKU_KEEP profiles through Sonnet ──
        if rescore_haiku_keeps:
            return self._rescore_haiku_keeps(
                master_df, enrichment_lookup,
                sonnet_batch_size=sonnet_batch_size,
                delay=delay, analysis_dir=analysis_dir,
            )

        # Initialize columns
        for col in ["ai_audience_fit", "ai_icp_tag", "ai_reasoning", "ai_decision"]:
            if col not in master_df.columns:
                master_df[col] = None

        # Tag real_network immediately
        real_mask = master_df.get("real_network", False) == True  # noqa
        master_df.loc[real_mask, "ai_audience_fit"] = 100
        master_df.loc[real_mask, "ai_icp_tag"] = "REAL_NETWORK"
        master_df.loc[real_mask, "ai_reasoning"] = "Active relationship (10+ messages)"
        master_df.loc[real_mask, "ai_decision"] = "KEEP"

        # ── Signal-based auto-keep pre-filter ──
        # Profiles with hard keep signals don't need AI — auto-keep them
        signal_cols = {
            "total_messages": lambda s: s.fillna(0).astype(float) > 0,
            "is_customer": lambda s: s.fillna(False).astype(bool),
            "is_former_customer": lambda s: s.fillna(False).astype(bool),
            "shared_school": lambda s: s.fillna(False).astype(bool),
            "shared_experience": lambda s: s.fillna(False).astype(bool),
            "is_target_account": lambda s: s.fillna(False).astype(bool),
            "is_target_prospect": lambda s: s.fillna(False).astype(bool),
            "total_engagements": lambda s: s.fillna(0).astype(float) > 0,
        }

        not_real = master_df.get("real_network", False) != True  # noqa
        not_scored = master_df["ai_decision"].isna()
        eligible = not_real & not_scored

        # Build per-signal masks and collect reasoning
        any_signal = eligible.copy() & False  # start all-False
        signal_reasons = {}  # pid -> list of signal names
        for col_name, check_fn in signal_cols.items():
            if col_name not in master_df.columns:
                continue
            col_mask = eligible & check_fn(master_df[col_name])
            any_signal = any_signal | col_mask
            for pid in master_df.loc[col_mask, "linkedin_profile_id"]:
                signal_reasons.setdefault(pid, []).append(col_name)

        signal_keep_count = any_signal.sum()
        if signal_keep_count > 0:
            master_df.loc[any_signal, "ai_audience_fit"] = 70
            master_df.loc[any_signal, "ai_icp_tag"] = "SIGNAL_KEEP"
            master_df.loc[any_signal, "ai_decision"] = "KEEP"
            # Write per-profile reasoning
            for pid, signals in signal_reasons.items():
                mask = master_df["linkedin_profile_id"] == pid
                master_df.loc[mask, "ai_reasoning"] = f"Auto-keep: {', '.join(signals)}"

        logger.info("Signal-based auto-keep: %d profiles (skipping AI)", signal_keep_count)

        # Profiles that need scoring (after real_network + signal keeps)
        needs_scoring = (
            (master_df.get("real_network", False) != True)  # noqa
            & (master_df["ai_decision"].isna())
        )
        to_score = master_df.loc[needs_scoring].copy()
        logger.info("Profiles to score: %d (skipping %d real_network, %d signal_keep)",
                     len(to_score), real_mask.sum(), signal_keep_count)

        if to_score.empty:
            return master_df

        # ── Load checkpoint ──
        checkpoint = self._load_checkpoint(analysis_dir, "haiku")
        if checkpoint:
            logger.info("Resuming Haiku: %d already triaged", len(checkpoint))

        # ── TIER 1: Haiku triage ──
        logger.info("=== TIER 1: Haiku triage (%d profiles, batches of %d) ===",
                     len(to_score), haiku_batch_size)

        profiles_for_haiku = []
        for _, row in to_score.iterrows():
            pid = row.get("linkedin_profile_id")
            if pid in checkpoint:
                continue
            profiles_for_haiku.append(self._build_lean_summary(row, enrichment_lookup))

        logger.info("Sending %d profiles to Haiku (after checkpoint skip)", len(profiles_for_haiku))

        total_to_score = len(profiles_for_haiku)
        haiku_results = dict(checkpoint)  # start from checkpoint
        batches = [
            profiles_for_haiku[i:i + haiku_batch_size]
            for i in range(0, len(profiles_for_haiku), haiku_batch_size)
        ]

        for batch_idx, batch in enumerate(batches):
            logger.info("Haiku batch %d/%d (%d profiles)", batch_idx + 1, len(batches), len(batch))

            results = self._haiku_classify(batch)
            for r in results:
                pid = r["id"]
                haiku_results[pid] = r

            if progress_callback:
                progress_callback(len(haiku_results), total_to_score, "Haiku triage")

            if (batch_idx + 1) % 10 == 0:
                self._save_checkpoint(analysis_dir, "haiku", haiku_results)
                logger.info("Haiku checkpoint: %d triaged", len(haiku_results))

            if batch_idx < len(batches) - 1:
                time.sleep(delay)

        # Final checkpoint
        self._save_checkpoint(analysis_dir, "haiku", haiku_results)

        # Apply Haiku results
        keep_count = sum(1 for r in haiku_results.values() if r["d"] == "KEEP")
        remove_count = sum(1 for r in haiku_results.values() if r["d"] == "REMOVE")
        review_count = sum(1 for r in haiku_results.values() if r["d"] == "REVIEW")

        logger.info("Haiku triage complete: %d KEEP, %d REMOVE, %d REVIEW",
                     keep_count, remove_count, review_count)

        # Apply KEEP and REMOVE directly
        for pid, r in haiku_results.items():
            mask = master_df["linkedin_profile_id"] == pid
            if not mask.any():
                continue
            master_df.loc[mask, "ai_decision"] = r["d"]
            master_df.loc[mask, "ai_reasoning"] = r.get("r", "")
            if r["d"] == "KEEP":
                master_df.loc[mask, "ai_audience_fit"] = 60  # default keep score
                master_df.loc[mask, "ai_icp_tag"] = "HAIKU_KEEP"
            elif r["d"] == "REMOVE":
                master_df.loc[mask, "ai_audience_fit"] = 15  # default remove score
                master_df.loc[mask, "ai_icp_tag"] = "NONE"

        # ── TIER 2: Sonnet deep-score on REVIEW cases ──
        review_pids = [pid for pid, r in haiku_results.items() if r["d"] == "REVIEW"]

        if review_pids:
            logger.info("=== TIER 2: Sonnet deep-score (%d REVIEW profiles) ===", len(review_pids))

            # Load Sonnet checkpoint
            sonnet_checkpoint = self._load_checkpoint(analysis_dir, "sonnet")
            if sonnet_checkpoint:
                logger.info("Resuming Sonnet: %d already scored", len(sonnet_checkpoint))

            review_rows = master_df[master_df["linkedin_profile_id"].isin(review_pids)]
            profiles_for_sonnet = []
            for _, row in review_rows.iterrows():
                pid = row.get("linkedin_profile_id")
                if pid in sonnet_checkpoint:
                    continue
                profiles_for_sonnet.append(
                    self._build_full_summary(row, enrichment_lookup)
                )

            logger.info("Sending %d profiles to Sonnet", len(profiles_for_sonnet))

            total_with_sonnet = len(haiku_results) + len(profiles_for_sonnet)
            sonnet_results = dict(sonnet_checkpoint)
            batches = [
                profiles_for_sonnet[i:i + sonnet_batch_size]
                for i in range(0, len(profiles_for_sonnet), sonnet_batch_size)
            ]

            for batch_idx, batch in enumerate(batches):
                logger.info("Sonnet batch %d/%d (%d profiles)",
                             batch_idx + 1, len(batches), len(batch))

                results = self._sonnet_score(batch)
                for r in results:
                    pid = r["linkedin_profile_id"]
                    sonnet_results[pid] = r

                if progress_callback:
                    progress_callback(len(haiku_results) + len(sonnet_results),
                                      total_with_sonnet, "Sonnet deep-score")

                if (batch_idx + 1) % 5 == 0:
                    self._save_checkpoint(analysis_dir, "sonnet", sonnet_results)

                if batch_idx < len(batches) - 1:
                    time.sleep(delay)

            self._save_checkpoint(analysis_dir, "sonnet", sonnet_results)

            # Apply Sonnet results
            for pid, r in sonnet_results.items():
                mask = master_df["linkedin_profile_id"] == pid
                if not mask.any():
                    continue
                score = r.get("audience_fit_score", 0)
                master_df.loc[mask, "ai_audience_fit"] = score
                master_df.loc[mask, "ai_icp_tag"] = r.get("icp_tag", "NONE")
                master_df.loc[mask, "ai_reasoning"] = r.get("reasoning", "")
                master_df.loc[mask, "ai_decision"] = "KEEP" if score >= 50 else "REMOVE"

            logger.info("Sonnet scoring complete: %d profiles deep-scored", len(sonnet_results))

        scored = master_df["ai_decision"].notna().sum()
        logger.info("Total scored: %d/%d", scored, len(master_df))

        return master_df

    # ── Profile summaries ─────────────────────────────────────────────────

    def _build_lean_summary(self, row, enrichment_lookup):
        """Minimal profile for Haiku: id, title, company, summary snippet."""
        pid = row.get("linkedin_profile_id")
        enrichment = enrichment_lookup.get(pid, {})
        summary = enrichment.get("summary", "") or ""
        if len(summary) > 200:
            summary = summary[:200] + "..."

        return {
            "id": pid,
            "t": row.get("current_job_title", "") or row.get("job_title", "") or "",
            "c": row.get("current_company", "") or "",
            "s": summary,
        }

    def _build_full_summary(self, row, enrichment_lookup):
        """Full profile for Sonnet (same as existing scorer)."""
        pid = row.get("linkedin_profile_id")
        enrichment = enrichment_lookup.get(pid, {})

        experience_summary = ""
        experiences = enrichment.get("experiences", [])
        if experiences and isinstance(experiences, list):
            parts = []
            for exp in experiences[:5]:
                if not isinstance(exp, dict):
                    continue
                title = exp.get("title", "")
                company = exp.get("company_name", "")
                date_start = exp.get("date_start", "")
                date_end = exp.get("date_end", "") or "present"
                if title or company:
                    parts.append(f"{title} @ {company} ({date_start}-{date_end})")
            experience_summary = ", ".join(parts)

        skills_raw = enrichment.get("skills", [])
        skills = []
        if skills_raw and isinstance(skills_raw, list):
            for s in skills_raw:
                if isinstance(s, dict):
                    skills.append(s.get("name", str(s)))
                elif isinstance(s, str):
                    skills.append(s)

        return {
            "linkedin_profile_id": pid,
            "full_name": row.get("full_name", ""),
            "current_job_title": row.get("current_job_title", "") or row.get("job_title", ""),
            "current_company": row.get("current_company", ""),
            "skills": skills[:15],
            "experience_summary": experience_summary,
            "is_follower": bool(row.get("is_follower", False)),
        }

    # ── API calls ─────────────────────────────────────────────────────────

    def _haiku_classify(self, batch):
        """Send batch to Haiku for KEEP/REMOVE/REVIEW classification."""
        profiles_text = json.dumps(batch, ensure_ascii=False)
        user_prompt = HAIKU_USER_TEMPLATE.format(count=len(batch), profiles=profiles_text)

        try:
            response = self.client.messages.create(
                model=self.haiku_model,
                max_tokens=4096,
                system=self._haiku_system,
                messages=[{"role": "user", "content": user_prompt}],
            )
        except Exception as e:
            logger.error("Haiku API failed: %s — retrying in 3s", e)
            time.sleep(3)
            try:
                response = self.client.messages.create(
                    model=self.haiku_model,
                    max_tokens=4096,
                    system=self._haiku_system,
                    messages=[{"role": "user", "content": user_prompt}],
                )
            except Exception as e2:
                logger.error("Haiku retry failed: %s — skipping batch", e2)
                return []

        return self._parse_haiku_response(response, batch)

    def _parse_haiku_response(self, response, original_batch):
        """Parse Haiku JSON array response."""
        text = response.content[0].text if response.content else ""

        code_block = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", text, re.DOTALL)
        json_str = code_block.group(1).strip() if code_block else text.strip()

        try:
            results = json.loads(json_str)
        except json.JSONDecodeError:
            logger.error("Haiku JSON parse failed. Raw: %s", text[:500])
            # Fall back: mark entire batch as REVIEW
            return [{"id": p["id"], "d": "REVIEW", "r": "parse error"} for p in original_batch]

        if not isinstance(results, list):
            return [{"id": p["id"], "d": "REVIEW", "r": "parse error"} for p in original_batch]

        valid_decisions = {"KEEP", "REMOVE", "REVIEW"}
        validated = []
        for r in results:
            if not isinstance(r, dict) or "id" not in r:
                continue
            d = str(r.get("d", "REVIEW")).upper()
            if d not in valid_decisions:
                d = "REVIEW"
            validated.append({
                "id": r["id"],
                "d": d,
                "r": str(r.get("r", ""))[:80],
            })

        return validated

    def _sonnet_score(self, batch):
        """Send batch to Sonnet for full scoring."""
        profiles_json = json.dumps(batch, ensure_ascii=False, indent=2)
        user_prompt = SONNET_USER_TEMPLATE.format(
            count=len(batch), profiles_json=profiles_json
        )

        try:
            response = self.client.messages.create(
                model=self.sonnet_model,
                max_tokens=4096,
                system=self._sonnet_system,
                messages=[{"role": "user", "content": user_prompt}],
            )
        except Exception as e:
            logger.error("Sonnet API failed: %s — retrying in 5s", e)
            time.sleep(5)
            try:
                response = self.client.messages.create(
                    model=self.sonnet_model,
                    max_tokens=4096,
                    system=self._sonnet_system,
                    messages=[{"role": "user", "content": user_prompt}],
                )
            except Exception as e2:
                logger.error("Sonnet retry failed: %s — skipping batch", e2)
                return []

        return self._parse_sonnet_response(response)

    def _parse_sonnet_response(self, response):
        """Parse Sonnet scoring response."""
        text = response.content[0].text if response.content else ""

        code_block = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", text, re.DOTALL)
        json_str = code_block.group(1).strip() if code_block else text.strip()

        try:
            results = json.loads(json_str)
        except json.JSONDecodeError:
            logger.error("Sonnet JSON parse failed. Raw: %s", text[:500])
            return []

        if not isinstance(results, list):
            return []

        valid_tags = {"DM", "C_LEVEL", "CHAMPION", "INFLUENCER", "INVESTOR", "MKTG_PARTNER", "NONE"}
        validated = []
        for r in results:
            if not isinstance(r, dict) or "linkedin_profile_id" not in r:
                continue
            tag = str(r.get("icp_tag", "NONE")).upper()
            if tag not in valid_tags:
                tag = "NONE"
            validated.append({
                "linkedin_profile_id": r["linkedin_profile_id"],
                "audience_fit_score": int(r.get("audience_fit_score", 0)),
                "icp_tag": tag,
                "reasoning": str(r.get("reasoning", "")),
            })

        return validated

    # ── Rescore HAIKU_KEEP via Sonnet ────────────────────────────────────

    def _rescore_haiku_keeps(self, master_df, enrichment_lookup,
                             sonnet_batch_size=20, delay=1.0,
                             analysis_dir=None):
        """
        Send all HAIKU_KEEP profiles through Sonnet deep scoring.

        Uses checkpoint tier 'sonnet_haiku_keep' so it won't collide
        with existing haiku/sonnet/sonnet_haiku_remove checkpoints.
        Interruptible — resume by calling again with the same master_df.
        """
        checkpoint_tier = "sonnet_haiku_keep"

        haiku_keep_mask = master_df["ai_icp_tag"] == "HAIKU_KEEP"

        # Exclude profiles with hard signals — scoring won't change their outcome
        signal_mask = (
            (master_df.get("total_messages", 0).fillna(0).astype(float) > 0) |
            (master_df.get("is_customer", False).fillna(False).astype(bool)) |
            (master_df.get("is_former_customer", False).fillna(False).astype(bool)) |
            (master_df.get("shared_school", False).fillna(False).astype(bool)) |
            (master_df.get("shared_experience", False).fillna(False).astype(bool)) |
            (master_df.get("is_target_account", False).fillna(False).astype(bool)) |
            (master_df.get("is_target_prospect", False).fillna(False).astype(bool)) |
            (master_df.get("total_engagements", 0).fillna(0).astype(float) > 0) |
            (master_df.get("real_network", False).fillna(False).astype(bool)) |
            (master_df.get("i_liked_their_posts", 0).fillna(0).astype(float) > 0) |
            (master_df.get("i_commented_their_posts", 0).fillna(0).astype(float) > 0)
        )

        haiku_keep_rows = master_df.loc[haiku_keep_mask & ~signal_mask]
        total = len(haiku_keep_rows)
        skipped = (haiku_keep_mask & signal_mask).sum()
        if skipped > 0:
            logger.info("Skipping %d HAIKU_KEEP profiles with hard signals", skipped)

        if total == 0:
            logger.info("No HAIKU_KEEP profiles to rescore.")
            return master_df

        logger.info("=== RESCORE: Sonnet deep-score on %d HAIKU_KEEP profiles ===", total)

        # Load checkpoint
        checkpoint = self._load_checkpoint(analysis_dir, checkpoint_tier)
        if checkpoint:
            logger.info("Resuming rescore: %d already scored", len(checkpoint))

        # Build profiles for Sonnet (skip already checkpointed)
        profiles_for_sonnet = []
        for _, row in haiku_keep_rows.iterrows():
            pid = row.get("linkedin_profile_id")
            if pid in checkpoint:
                continue
            profiles_for_sonnet.append(
                self._build_full_summary(row, enrichment_lookup)
            )

        logger.info("Sending %d profiles to Sonnet (%d already done)",
                     len(profiles_for_sonnet), len(checkpoint))

        if not profiles_for_sonnet:
            logger.info("All HAIKU_KEEP profiles already scored. Applying results.")
        else:
            sonnet_results = dict(checkpoint)
            batches = [
                profiles_for_sonnet[i:i + sonnet_batch_size]
                for i in range(0, len(profiles_for_sonnet), sonnet_batch_size)
            ]

            for batch_idx, batch in enumerate(batches):
                logger.info("Rescore batch %d/%d (%d profiles)",
                             batch_idx + 1, len(batches), len(batch))

                results = self._sonnet_score(batch)
                for r in results:
                    pid = r["linkedin_profile_id"]
                    sonnet_results[pid] = r

                # Save checkpoint every batch (not every 5 — we want max safety)
                self._save_checkpoint(analysis_dir, checkpoint_tier, sonnet_results)

                if batch_idx < len(batches) - 1:
                    time.sleep(delay)

            checkpoint = sonnet_results

        # Apply all results (from checkpoint + new) to master_df
        applied = 0
        for pid, r in checkpoint.items():
            mask = master_df["linkedin_profile_id"] == pid
            if not mask.any():
                continue
            score = r.get("audience_fit_score", 0)
            master_df.loc[mask, "ai_audience_fit"] = score
            master_df.loc[mask, "ai_icp_tag"] = r.get("icp_tag", "NONE")
            master_df.loc[mask, "ai_reasoning"] = r.get("reasoning", "")
            master_df.loc[mask, "ai_decision"] = "KEEP" if score >= 50 else "REMOVE"
            applied += 1

        logger.info("Rescore complete: %d/%d profiles deep-scored", applied, total)
        return master_df

    # ── Checkpointing ─────────────────────────────────────────────────────

    def _save_checkpoint(self, analysis_dir, tier, results):
        """Save checkpoint for a tier (haiku or sonnet)."""
        path = Path(analysis_dir) / f"ai_scores_{tier}_checkpoint.json"
        # Convert dict values to list for JSON
        results_list = list(results.values()) if isinstance(results, dict) else results
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tier": tier,
            "total": len(results_list),
            "results": results_list,
        }
        path.write_text(json.dumps(payload, default=str, ensure_ascii=False), encoding="utf-8")
        logger.debug("Checkpoint saved: %s (%d results)", path.name, len(results_list))

    def _load_checkpoint(self, analysis_dir, tier):
        """Load checkpoint for a tier. Returns dict {profile_id: result}."""
        path = Path(analysis_dir) / f"ai_scores_{tier}_checkpoint.json"
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            results = data.get("results", [])
            # Key by profile ID
            key = "id" if tier == "haiku" else "linkedin_profile_id"
            return {r[key]: r for r in results if key in r}
        except Exception as e:
            logger.warning("Failed to load %s checkpoint: %s", tier, e)
            return {}
