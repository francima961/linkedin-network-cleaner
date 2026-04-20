"""
Network analysis layer for the LinkedIn Audience Cleaner Agent.

Builds a master DataFrame of connections enriched with follower status,
inbox activity, post engagement, content interactions, enrichment data,
and target account/prospect matching.

Each step method loads its own extracts and left-joins results onto the
master DataFrame. Steps are designed to run sequentially but tolerate
missing data gracefully.

Usage:
    from linkedin_network_cleaner.core.analyzer import NetworkAnalyzer
    from linkedin_network_cleaner.core import config

    analyzer = NetworkAnalyzer(config.EXTRACTS_DIR, config.ASSETS_DIR, config.ANALYSIS_DIR)
    master_df, followers_aside_df = analyzer.build_base()
    master_df = analyzer.analyze_inbox(master_df)
    master_df = analyzer.analyze_post_engagement(master_df)
    master_df = analyzer.analyze_content_interactions(master_df)
    master_df = analyzer.enrich_for_matching(master_df, profile_url="https://linkedin.com/in/...")
    master_df = analyzer.match_target_accounts(master_df)
    master_df = analyzer.match_target_prospects(master_df)
"""

import json
import glob as globmod
import logging
import re
from pathlib import Path

import pandas as pd

from . import config
from .edges_client import EdgesClient

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

# Suffixes to strip when normalizing company names for matching
_COMPANY_SUFFIXES = re.compile(
    r"\b(inc\.?|llc\.?|ltd\.?|corp\.?|co\.?|gmbh|s\.?a\.?|plc|pty|n\.?v\.?|"
    r"limited|incorporated|corporation|company|group|holdings?)\s*$",
    re.IGNORECASE,
)

# Candidate column names for auto-detection
_PROSPECT_ID_CANDIDATES = [
    "Person Linkedin Id",
    "LinkedIn Member ID",
    "linkedin_profile_id",
    "LinkedIn ID",
    "li_id",
]

_ACCOUNT_NAME_CANDIDATES = [
    "Company",
    "Company Name",
    "company_name",
    "Organization",
    "Account Name",
]

_PROSPECT_ICP_CANDIDATES = [
    "ICP Tag",
    "persona_tag",
    "Persona",
    "icp_tag",
    "ICP",
    "persona",
    "Persona Tag",
]


class NetworkAnalyzer:
    """Analyzes LinkedIn network data from extracted JSON files."""

    def __init__(self, extracts_dir: Path, assets_dir: Path, analysis_dir: Path):
        self.extracts_dir = Path(extracts_dir)
        self.assets_dir = Path(assets_dir)
        self.analysis_dir = Path(analysis_dir)

    # ── Helper: load latest extract ──────────────────────────────────────

    def _load_latest_extract(self, name: str):
        """
        Load the most recent extract by name prefix.
        Returns the 'data' field from the JSON payload, or None if not found.
        """
        pattern = str(self.extracts_dir / f"{name}_*.json")
        # Exclude checkpoint files
        files = sorted(
            f for f in globmod.glob(pattern)
            if "_checkpoint_" not in Path(f).name
        )
        if not files:
            logger.warning("No extract found for '%s'", name)
            return None

        latest = files[-1]
        logger.info("Loading latest '%s' extract: %s", name, Path(latest).name)
        with open(latest, encoding="utf-8") as f:
            payload = json.load(f)
        return payload.get("data")

    # ── Step 1: Build Base ───────────────────────────────────────────────

    def build_base(self):
        """
        Build the master DataFrame from connections + followers.

        Returns:
            (master_df, followers_aside_df)
            - master_df: all connections with follower flags
            - followers_aside_df: followers who are NOT connections
        """
        # Load connections
        connections_data = self._load_latest_extract("connections")
        if connections_data is None or len(connections_data) == 0:
            raise ValueError(
                "No connections extract found. Run extraction first."
            )

        master_df = pd.DataFrame(connections_data)
        master_df["linkedin_profile_id"] = pd.to_numeric(
            master_df["linkedin_profile_id"], errors="coerce"
        ).astype("Int64")
        logger.info("Connections loaded: %d rows", len(master_df))

        # Load followers (optional)
        followers_data = self._load_latest_extract("followers")
        if followers_data is None or len(followers_data) == 0:
            logger.warning("No followers extract found — skipping follower flags")
            master_df["is_follower"] = False
            master_df["is_following"] = False
            master_df["is_mutual_follower"] = False
            followers_aside_df = pd.DataFrame(
                columns=["linkedin_profile_id", "full_name", "headline",
                         "linkedin_profile_url", "in_network"]
            )
            return master_df, followers_aside_df

        followers_df = pd.DataFrame(followers_data)
        followers_df["linkedin_profile_id"] = pd.to_numeric(
            followers_df["linkedin_profile_id"], errors="coerce"
        ).astype("Int64")
        logger.info("Followers loaded: %d rows", len(followers_df))

        # Build follower ID set
        follower_ids = set(followers_df["linkedin_profile_id"].dropna())

        # is_follower: this person follows the user
        master_df["is_follower"] = master_df["linkedin_profile_id"].isin(follower_ids)

        # is_following: check if the follower record indicates the user follows them
        # Follower data may have a field like "is_following" or "following"
        following_ids = set()
        if "is_following" in followers_df.columns:
            following_ids = set(
                followers_df.loc[
                    followers_df["is_following"].astype(bool),
                    "linkedin_profile_id"
                ].dropna()
            )
        elif "following" in followers_df.columns:
            following_ids = set(
                followers_df.loc[
                    followers_df["following"].astype(bool),
                    "linkedin_profile_id"
                ].dropna()
            )
        master_df["is_following"] = master_df["linkedin_profile_id"].isin(following_ids)

        # Mutual = both directions
        master_df["is_mutual_follower"] = (
            master_df["is_follower"] & master_df["is_following"]
        )

        # Followers aside: followers NOT in connections
        connection_ids = set(master_df["linkedin_profile_id"].dropna())
        aside_mask = ~followers_df["linkedin_profile_id"].isin(connection_ids)
        followers_aside_df = followers_df.loc[aside_mask].copy()

        # Normalize columns for the aside DataFrame
        aside_cols = {
            "linkedin_profile_id": "linkedin_profile_id",
            "full_name": "full_name",
            "headline": "headline",
            "linkedin_profile_url": "linkedin_profile_url",
        }
        keep_cols = [c for c in aside_cols if c in followers_aside_df.columns]
        followers_aside_df = followers_aside_df[keep_cols].copy()
        followers_aside_df["in_network"] = False

        logger.info(
            "Build base complete: %d connections, %d followers aside",
            len(master_df), len(followers_aside_df),
        )
        return master_df, followers_aside_df

    # ── Step 2: Analyze Inbox ────────────────────────────────────────────

    def analyze_inbox(self, master_df, dm_threshold=5, **_kwargs):
        """
        Analyze conversation/message data to determine 'active_dms' connections.

        A connection has active DMs if:
            total_messages >= dm_threshold AND
            their_messages >= 1 AND my_messages >= 1

        Adds columns: conversation_count, total_messages, their_messages,
                      my_messages, active_dms
        """
        # Load conversations (thread list with participants)
        conversations_data = self._load_latest_extract("conversations")
        if conversations_data is None:
            logger.warning("No conversations extract — adding default inbox columns")
            return self._add_default_inbox_columns(master_df)

        # Load per-thread messages
        messages_data = self._load_latest_extract("messages_by_thread")
        if messages_data is None:
            logger.warning("No messages_by_thread extract — adding default inbox columns")
            return self._add_default_inbox_columns(master_df)

        # Build thread_id → list of participant profile IDs
        thread_participants = {}
        for conv in conversations_data:
            thread_id = conv.get("linkedin_thread_id", "")
            participants = conv.get("participants", [])
            participant_ids = set()
            for p in participants:
                pid = p.get("linkedin_profile_id") if isinstance(p, dict) else p
                if pid is not None:
                    try:
                        participant_ids.add(int(pid))
                    except (ValueError, TypeError):
                        pass
            thread_participants[thread_id] = participant_ids

        # Auto-detect the user's own linkedin_profile_id from message data.
        # The user's ID appears across the most threads (they're in every conversation).
        my_profile_id = self._detect_my_profile_id(messages_data, thread_participants)

        # Per-connection message counts
        # messages_data is a dict: thread_id → [message, ...]
        connection_stats = {}  # linkedin_profile_id → {their_msgs, my_msgs, conv_count}

        for thread_id, messages in messages_data.items():
            if not isinstance(messages, list):
                continue

            participants = thread_participants.get(thread_id, set())

            # Classify each message as "mine" or "theirs"
            my_count = 0
            their_count = 0

            for msg in messages:
                # Messages have sender's linkedin_profile_id as a top-level field
                sender_pid = msg.get("linkedin_profile_id")
                try:
                    sender_pid = int(sender_pid) if sender_pid is not None else None
                except (ValueError, TypeError):
                    sender_pid = None

                if sender_pid is not None and sender_pid == my_profile_id:
                    my_count += 1
                else:
                    their_count += 1

            # Attribute this thread's stats to each non-me participant
            for pid in participants:
                if pid not in connection_stats:
                    connection_stats[pid] = {
                        "their_messages": 0,
                        "my_messages": 0,
                        "conversation_count": 0,
                        "total_messages": 0,
                    }
                connection_stats[pid]["their_messages"] += their_count
                connection_stats[pid]["my_messages"] += my_count
                connection_stats[pid]["conversation_count"] += 1
                connection_stats[pid]["total_messages"] += their_count + my_count

        # Build stats DataFrame
        if connection_stats:
            stats_df = pd.DataFrame.from_dict(connection_stats, orient="index")
            stats_df.index.name = "linkedin_profile_id"
            stats_df = stats_df.reset_index()
            stats_df["linkedin_profile_id"] = pd.to_numeric(
                stats_df["linkedin_profile_id"], errors="coerce"
            ).astype("Int64")
        else:
            stats_df = pd.DataFrame(
                columns=["linkedin_profile_id", "conversation_count",
                         "total_messages", "their_messages", "my_messages"]
            )

        # Merge into master
        inbox_cols = ["conversation_count", "total_messages", "their_messages", "my_messages"]
        # Drop existing columns to avoid _x/_y suffixes on re-runs
        master_df = master_df.drop(
            columns=[c for c in inbox_cols + ["active_dms"] if c in master_df.columns],
            errors="ignore",
        )
        master_df = master_df.merge(
            stats_df[["linkedin_profile_id"] + inbox_cols],
            on="linkedin_profile_id",
            how="left",
        )

        # Fill NaN with 0 for connections with no conversations
        for col in inbox_cols:
            master_df[col] = master_df[col].fillna(0).astype(int)

        # Compute active_dms flag (both parties sent at least 1, total >= threshold)
        master_df["active_dms"] = (
            (master_df["total_messages"] >= dm_threshold)
            & (master_df["their_messages"] >= 1)
            & (master_df["my_messages"] >= 1)
        )

        logger.info(
            "Inbox analysis complete: %d active DM relationships out of %d (threshold=%d)",
            master_df["active_dms"].sum(), len(master_df), dm_threshold,
        )
        return master_df

    @staticmethod
    def _detect_my_profile_id(messages_data, thread_participants):
        """
        Auto-detect the user's linkedin_profile_id from message data.
        The user's ID is the sender that appears across the most threads
        AND is NOT listed as a thread participant (participants only list
        the other person).
        """
        from collections import Counter

        # Count how many threads each sender_pid appears in
        pid_thread_count = Counter()
        all_participant_ids = set()
        for pids in thread_participants.values():
            all_participant_ids.update(pids)

        for thread_id, messages in messages_data.items():
            if not isinstance(messages, list):
                continue
            thread_senders = set()
            for msg in messages:
                pid = msg.get("linkedin_profile_id")
                if pid is not None:
                    try:
                        thread_senders.add(int(pid))
                    except (ValueError, TypeError):
                        pass
            for pid in thread_senders:
                pid_thread_count[pid] += 1

        # The user's ID appears in most threads and is NOT a listed participant
        for pid, count in pid_thread_count.most_common(5):
            if pid not in all_participant_ids:
                logger.info("Auto-detected user profile ID: %d (appeared in %d threads)", pid, count)
                return pid

        # Fallback: just the most common sender
        if pid_thread_count:
            pid, count = pid_thread_count.most_common(1)[0]
            logger.warning("Could not distinguish user ID from participants, using most common: %d (%d threads)", pid, count)
            return pid

        return None

    def _add_default_inbox_columns(self, df):
        """Add zeroed inbox columns when no conversation data is available."""
        df["conversation_count"] = 0
        df["total_messages"] = 0
        df["their_messages"] = 0
        df["my_messages"] = 0
        df["active_dms"] = False
        return df

    # ── Step 3: Analyze Post Engagement ──────────────────────────────────

    def analyze_post_engagement(self, master_df):
        """
        Count how many times each connection liked/commented/reposted the user's posts.

        Adds columns: times_liked, times_commented, times_reposted, total_engagements
        """
        engagement_data = self._load_latest_extract("post_engagement_by_post")
        if engagement_data is None:
            logger.warning("No post_engagement_by_post extract — adding default columns")
            return self._add_default_engagement_columns(master_df)

        # engagement_data is: {"likers": {post_id: [profiles]}, "commenters": {...}, "reposters": {...}}
        likers_by_post = engagement_data.get("likers", {})
        commenters_by_post = engagement_data.get("commenters", {})
        reposters_by_post = engagement_data.get("reposters", {})

        # Count per-person engagement across all posts
        like_counts = self._count_engagement_by_profile(likers_by_post)
        comment_counts = self._count_engagement_by_profile(commenters_by_post)
        repost_counts = self._count_engagement_by_profile(reposters_by_post)

        # Build engagement DataFrame
        all_pids = set(like_counts) | set(comment_counts) | set(repost_counts)
        if not all_pids:
            logger.warning("No engagement data found across posts")
            return self._add_default_engagement_columns(master_df)

        eng_rows = []
        for pid in all_pids:
            eng_rows.append({
                "linkedin_profile_id": pid,
                "times_liked": like_counts.get(pid, 0),
                "times_commented": comment_counts.get(pid, 0),
                "times_reposted": repost_counts.get(pid, 0),
            })

        eng_df = pd.DataFrame(eng_rows)
        eng_df["linkedin_profile_id"] = pd.to_numeric(
            eng_df["linkedin_profile_id"], errors="coerce"
        ).astype("Int64")
        eng_df["total_engagements"] = (
            eng_df["times_liked"] + eng_df["times_commented"] + eng_df["times_reposted"]
        )

        # Merge into master
        eng_cols = ["times_liked", "times_commented", "times_reposted", "total_engagements"]
        master_df = master_df.drop(
            columns=[c for c in eng_cols if c in master_df.columns],
            errors="ignore",
        )
        master_df = master_df.merge(
            eng_df[["linkedin_profile_id"] + eng_cols],
            on="linkedin_profile_id",
            how="left",
        )

        for col in eng_cols:
            master_df[col] = master_df[col].fillna(0).astype(int)

        logger.info(
            "Post engagement analysis complete: %d connections with engagement",
            (master_df["total_engagements"] > 0).sum(),
        )
        return master_df

    @staticmethod
    def _count_engagement_by_profile(posts_dict):
        """
        Count engagement per linkedin_profile_id across all posts.
        posts_dict: {post_id: [profile_dicts or commenter_dicts, ...]}
        """
        counts = {}
        for _post_id, profiles in posts_dict.items():
            if not isinstance(profiles, list):
                continue
            for profile in profiles:
                pid = None
                if isinstance(profile, dict):
                    pid = profile.get("linkedin_profile_id")
                if pid is not None:
                    try:
                        pid = int(pid)
                    except (ValueError, TypeError):
                        continue
                    counts[pid] = counts.get(pid, 0) + 1
        return counts

    def _add_default_engagement_columns(self, df):
        """Add zeroed engagement columns when no post engagement data exists."""
        df["times_liked"] = 0
        df["times_commented"] = 0
        df["times_reposted"] = 0
        df["total_engagements"] = 0
        return df

    # ── Step 4: Analyze Content Interactions ─────────────────────────────

    def analyze_content_interactions(self, master_df):
        """
        Analyze the user's own reaction/comment activity on other people's posts.
        Counts how many times the user liked or commented on each connection's posts.

        Adds columns: i_liked_their_posts, i_commented_their_posts
        """
        reaction_data = self._load_latest_extract("reaction_activity")
        comment_data = self._load_latest_extract("comment_activity")

        if reaction_data is None and comment_data is None:
            logger.warning("No reaction/comment activity extracts — adding default columns")
            master_df["i_liked_their_posts"] = 0
            master_df["i_commented_their_posts"] = 0
            return master_df

        # Build handle → linkedin_profile_id lookup from master_df
        handle_to_pid = self._build_handle_lookup(master_df)

        # Count reactions (likes) per author
        like_counts = {}
        if reaction_data is not None:
            for record in reaction_data:
                author_pid = self._extract_author_from_post(record, handle_to_pid)
                if author_pid is not None:
                    like_counts[author_pid] = like_counts.get(author_pid, 0) + 1

        # Count comments per author
        comment_counts = {}
        if comment_data is not None:
            for record in comment_data:
                author_pid = self._extract_author_from_post(record, handle_to_pid)
                if author_pid is not None:
                    comment_counts[author_pid] = comment_counts.get(author_pid, 0) + 1

        # Build interactions DataFrame
        all_pids = set(like_counts) | set(comment_counts)
        if not all_pids:
            logger.warning("No content interaction matches found")
            master_df["i_liked_their_posts"] = 0
            master_df["i_commented_their_posts"] = 0
            return master_df

        rows = []
        for pid in all_pids:
            rows.append({
                "linkedin_profile_id": pid,
                "i_liked_their_posts": like_counts.get(pid, 0),
                "i_commented_their_posts": comment_counts.get(pid, 0),
            })

        interactions_df = pd.DataFrame(rows)
        interactions_df["linkedin_profile_id"] = pd.to_numeric(
            interactions_df["linkedin_profile_id"], errors="coerce"
        ).astype("Int64")

        # Merge
        int_cols = ["i_liked_their_posts", "i_commented_their_posts"]
        master_df = master_df.drop(
            columns=[c for c in int_cols if c in master_df.columns],
            errors="ignore",
        )
        master_df = master_df.merge(
            interactions_df[["linkedin_profile_id"] + int_cols],
            on="linkedin_profile_id",
            how="left",
        )

        for col in int_cols:
            master_df[col] = master_df[col].fillna(0).astype(int)

        logger.info(
            "Content interactions complete: liked %d connections' posts, "
            "commented on %d connections' posts",
            (master_df["i_liked_their_posts"] > 0).sum(),
            (master_df["i_commented_their_posts"] > 0).sum(),
        )
        return master_df

    @staticmethod
    def _build_handle_lookup(master_df):
        """
        Build a mapping from LinkedIn handle (slug) to linkedin_profile_id.
        Extracts the handle from linkedin_profile_url column.
        """
        lookup = {}
        if "linkedin_profile_url" not in master_df.columns:
            return lookup

        for _, row in master_df.iterrows():
            url = row.get("linkedin_profile_url")
            pid = row.get("linkedin_profile_id")
            if pd.isna(url) or pd.isna(pid):
                continue
            handle = _extract_handle_from_url(str(url))
            if handle:
                lookup[handle.lower()] = int(pid)
        return lookup

    @staticmethod
    def _extract_author_from_post(record, handle_to_pid):
        """
        Extract the post author's linkedin_profile_id from a reaction/comment record.
        Tries direct profile ID fields first, then falls back to URL handle parsing.
        """
        if not isinstance(record, dict):
            return None

        # Direct author profile ID
        author_pid = record.get("author_linkedin_profile_id")
        if author_pid is not None:
            try:
                return int(author_pid)
            except (ValueError, TypeError):
                pass

        # Extract handle from post URL or author URL
        for url_field in ("linkedin_post_url", "post_url", "author_url",
                          "author_linkedin_profile_url"):
            url = record.get(url_field)
            if not url:
                continue
            handle = _extract_handle_from_url(str(url))
            if handle and handle.lower() in handle_to_pid:
                return handle_to_pid[handle.lower()]

        return None

    # ── Step 5: Enrich for Matching ──────────────────────────────────────

    def enrich_for_matching(self, master_df, profile_url):
        """
        Merge enrichment data and compute shared_school / shared_experience flags.

        Args:
            master_df: current master DataFrame
            profile_url: the user's own LinkedIn profile URL (to fetch their schools/experiences)

        Adds columns: shared_school, shared_experience, current_job_title, current_company
        """
        enrichment_data = self._load_latest_extract("enrichment")
        if enrichment_data is None:
            logger.warning("No enrichment extract — adding default columns")
            return self._add_default_enrichment_columns(master_df)

        # Fetch user's own profile for school/experience comparison
        my_school_ids, my_school_names, my_experiences = self._fetch_user_profile(profile_url)
        logger.info(
            "User schools — %d IDs: %s, %d name-only: %s",
            len(my_school_ids), my_school_ids,
            len(my_school_names), my_school_names,
        )

        # Build enrichment DataFrame
        enrich_rows = []
        for profile in enrichment_data:
            if not isinstance(profile, dict):
                continue

            pid = profile.get("linkedin_profile_id")
            if pid is None:
                continue

            try:
                pid = int(pid)
            except (ValueError, TypeError):
                continue

            # Shared school check — ID-based primary, name-contains fallback
            shared_school = self._has_shared_school(
                my_school_ids, my_school_names, profile,
            )

            # Shared experience check (overlapping tenure at same company)
            their_experiences = self._extract_experiences(profile)
            shared_experience = self._has_overlapping_experience(
                my_experiences, their_experiences
            )

            enrich_rows.append({
                "linkedin_profile_id": pid,
                "shared_school": shared_school,
                "shared_experience": shared_experience,
                "current_job_title": profile.get("current_job_title", ""),
                "current_company": profile.get("current_company", ""),
            })

        if not enrich_rows:
            logger.warning("No enrichment records matched — adding default columns")
            return self._add_default_enrichment_columns(master_df)

        enrich_df = pd.DataFrame(enrich_rows)
        enrich_df["linkedin_profile_id"] = pd.to_numeric(
            enrich_df["linkedin_profile_id"], errors="coerce"
        ).astype("Int64")

        # Merge
        enrich_cols = ["shared_school", "shared_experience",
                       "current_job_title", "current_company"]
        master_df = master_df.drop(
            columns=[c for c in enrich_cols if c in master_df.columns],
            errors="ignore",
        )
        master_df = master_df.merge(
            enrich_df[["linkedin_profile_id"] + enrich_cols],
            on="linkedin_profile_id",
            how="left",
        )

        # Defaults for non-enriched connections
        master_df["shared_school"] = master_df["shared_school"].fillna(False)
        master_df["shared_experience"] = master_df["shared_experience"].fillna(False)
        master_df["current_job_title"] = master_df["current_job_title"].fillna("")
        master_df["current_company"] = master_df["current_company"].fillna("")

        logger.info(
            "Enrichment merge complete: %d shared_school, %d shared_experience",
            master_df["shared_school"].sum(),
            master_df["shared_experience"].sum(),
        )
        return master_df

    def _fetch_user_profile(self, profile_url):
        """
        Fetch the user's own profile to extract schools and experiences.
        Educations require the dedicated extract-people-educations endpoint;
        extract-people does not return them.
        Returns (school_ids_set, school_names_set, experiences_list).
        School IDs are used for primary matching; names are fallback for schools without IDs.
        """
        my_school_ids = set()
        my_school_names = set()
        my_experiences = []

        try:
            client = EdgesClient(
                api_key=config.API_KEY,
                identity_uuid=config.IDENTITY_UUID,
                base_url=config.BASE_URL,
            )

            # Fetch experiences via extract-people
            data, _, error = client.call_action(
                "extract-people",
                input_data={"url": profile_url},
                direct_mode=False,
                parameters={"experiences": True},
            )

            if error is not None:
                logger.warning(
                    "Could not fetch user profile for matching: %s",
                    error.get("error_label", "UNKNOWN"),
                )
            elif data is not None:
                profile = data if isinstance(data, dict) else (
                    data[0] if isinstance(data, list) and data else {}
                )
                my_experiences = self._extract_experiences(profile)

            # Fetch educations via dedicated endpoint (extract-people doesn't return them)
            edu_data, _, edu_error = client.call_action(
                "extract-people-educations",
                input_data={"url": profile_url},
                direct_mode=False,
            )

            if edu_error is not None:
                logger.warning(
                    "Could not fetch user educations: %s",
                    edu_error.get("error_label", "UNKNOWN"),
                )
            elif edu_data is not None:
                edu_profile = edu_data if isinstance(edu_data, dict) else (
                    edu_data[0] if isinstance(edu_data, list) and edu_data else {}
                )
                my_school_ids, my_school_names = self._extract_school_ids_and_names(
                    edu_profile
                )

            logger.info(
                "User profile: %d school IDs, %d school names (no ID), %d experiences",
                len(my_school_ids), len(my_school_names), len(my_experiences),
            )

        except Exception:
            logger.exception("Error fetching user profile for matching")

        return my_school_ids, my_school_names, my_experiences

    @staticmethod
    def _extract_school_names(profile):
        """Extract a set of normalized school names from a profile dict."""
        schools = set()
        educations = profile.get("educations", []) or profile.get("education", []) or []
        for edu in educations:
            if not isinstance(edu, dict):
                continue
            name = edu.get("school_name") or edu.get("school") or edu.get("name", "")
            if name:
                schools.add(name.strip().lower())
        return schools

    @staticmethod
    def _extract_school_ids_and_names(profile):
        """
        Extract school IDs and fallback names from a profile's education entries.
        Returns (school_ids_set, school_names_set).
        school_names_set only contains names for schools that have NO linkedin_school_id.
        """
        school_ids = set()
        school_names = set()
        educations = profile.get("educations", []) or profile.get("education", []) or []
        for edu in educations:
            if not isinstance(edu, dict):
                continue
            sid = edu.get("linkedin_school_id")
            if sid:
                school_ids.add(int(sid))
            else:
                # No ID — use name as fallback
                name = edu.get("school_name") or edu.get("school") or edu.get("name", "")
                if name:
                    school_names.add(name.strip().lower())
        return school_ids, school_names

    @staticmethod
    def _has_shared_school(my_school_ids, my_school_names, profile):
        """
        Check if a profile shares a school with the user.
        Primary: match on linkedin_school_id (reliable, handles name variants).
        Fallback: name-contains for user schools that have no ID.
        """
        educations = profile.get("educations", []) or profile.get("education", []) or []
        for edu in educations:
            if not isinstance(edu, dict):
                continue
            # ID-based match (primary)
            sid = edu.get("linkedin_school_id")
            if sid and int(sid) in my_school_ids:
                return True
            # Name-based fallback (only for user schools without IDs)
            if my_school_names:
                name = edu.get("school_name") or edu.get("school") or edu.get("name", "")
                if name:
                    name_lower = name.strip().lower()
                    for my_name in my_school_names:
                        if my_name in name_lower or name_lower in my_name:
                            return True
        return False

    @staticmethod
    def _extract_experiences(profile):
        """
        Extract experiences as a list of dicts with company_id, company_name, start, end.
        Dates are parsed to (year, month) tuples where possible.
        Uses linkedin_company_id as primary key, company_name as fallback.
        """
        experiences = []
        raw = profile.get("experiences", []) or []
        for exp in raw:
            if not isinstance(exp, dict):
                continue
            company = exp.get("company_name") or exp.get("company", "")
            company_id = exp.get("linkedin_company_id")
            if not company and not company_id:
                continue
            start = _parse_date(exp.get("date_start") or exp.get("start_date", ""))
            end = _parse_date(exp.get("date_end") or exp.get("end_date", ""))
            experiences.append({
                "company_id": int(company_id) if company_id else None,
                "company_name": company.strip().lower() if company else "",
                "start": start,
                "end": end,
            })
        return experiences

    @staticmethod
    def _has_overlapping_experience(my_experiences, their_experiences):
        """
        Check if any two experiences share the same company with overlapping tenure.
        Primary: match on linkedin_company_id.
        Fallback: name-contains match (handles subsidiaries, regional entities,
        and cases where the same company has different LinkedIn IDs).
        """
        for my_exp in my_experiences:
            for their_exp in their_experiences:
                # ID-based match (primary)
                same_company = False
                if my_exp["company_id"] and their_exp["company_id"]:
                    if my_exp["company_id"] == their_exp["company_id"]:
                        same_company = True

                # Name-based match (fallback — always checked if ID didn't match)
                if not same_company and my_exp["company_name"] and their_exp["company_name"]:
                    same_company = (
                        my_exp["company_name"] == their_exp["company_name"]
                        or my_exp["company_name"] in their_exp["company_name"]
                        or their_exp["company_name"] in my_exp["company_name"]
                    )

                if not same_company:
                    continue
                # Check date overlap
                if _dates_overlap(
                    my_exp["start"], my_exp["end"],
                    their_exp["start"], their_exp["end"],
                ):
                    return True
        return False

    def _add_default_enrichment_columns(self, df):
        """Add default enrichment columns when no enrichment data exists."""
        df["shared_school"] = False
        df["shared_experience"] = False
        df["current_job_title"] = ""
        df["current_company"] = ""
        return df

    # ── Step 6: Match Customers ────────────────────────────────────────

    def match_customers(self, master_df):
        """
        Match connections against customer company lists using both
        current_company AND past experience history from enrichment data.

        Adds columns:
            is_customer        — currently works at a customer company
            is_former_customer — previously worked at a customer company (but not currently)
        """
        customer_lookup = self._build_customer_lookup()

        if not customer_lookup:
            master_df["is_customer"] = False
            master_df["is_former_customer"] = False
            return master_df

        # --- Current company matching ---
        company_col = "current_company" if "current_company" in master_df.columns else (
            "company" if "company" in master_df.columns else None
        )

        # Drop existing columns to avoid issues on re-runs
        master_df = master_df.drop(
            columns=["is_customer", "is_former_customer"], errors="ignore"
        )

        if company_col:
            master_df["is_customer"] = master_df[company_col].apply(
                lambda c: _match_customer_name(c, customer_lookup)
            )
        else:
            logger.warning("No company column available for current customer matching")
            master_df["is_customer"] = False

        # --- Past experience matching from enrichment data ---
        enrichment_data = self._load_latest_extract("enrichment")
        if enrichment_data is None:
            logger.warning("No enrichment data — skipping former customer matching")
            master_df["is_former_customer"] = False
        else:
            # Pre-compute: for each profile, check if ANY past experience
            # matches a customer company name
            former_customer_pids = set()
            for profile in enrichment_data:
                if not isinstance(profile, dict):
                    continue
                pid = profile.get("linkedin_profile_id")
                if pid is None:
                    continue
                try:
                    pid = int(pid)
                except (ValueError, TypeError):
                    continue

                experiences = profile.get("experiences", []) or []
                for exp in experiences:
                    if not isinstance(exp, dict):
                        continue
                    company = exp.get("company_name") or exp.get("company", "")
                    if not company:
                        continue
                    if _match_customer_name(company, customer_lookup):
                        former_customer_pids.add(pid)
                        break  # one match is enough

            master_df["is_former_customer"] = (
                master_df["linkedin_profile_id"].apply(
                    lambda pid: int(pid) in former_customer_pids
                    if pd.notna(pid) else False
                )
                # Exclude current customers — "former" means not currently there
                & ~master_df["is_customer"]
            )

            logger.info(
                "Former customer matching: %d connections previously at customer companies",
                master_df["is_former_customer"].sum(),
            )

        logger.info(
            "Customer matching complete: %d current, %d former",
            master_df["is_customer"].sum(),
            master_df["is_former_customer"].sum(),
        )
        return master_df

    def _build_customer_lookup(self):
        """Parse customer CSVs and return a set of normalized company names."""
        customers_dir = self.assets_dir / "Customers"
        if not customers_dir.exists():
            logger.warning("Customers directory not found at %s", customers_dir)
            return set()

        csv_files = list(customers_dir.glob("*.csv"))
        if not csv_files:
            logger.warning("No CSV files found in %s", customers_dir)
            return set()

        customer_names_raw = set()
        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file, encoding="utf-8", low_memory=False)
                name_col = _detect_column(
                    df,
                    ["company_name", "Company Name", "Company", "company"],
                    "customer company name",
                )
                names = df[name_col].dropna().astype(str).str.strip()
                names = names[names != ""]
                customer_names_raw.update(names)
                logger.info("Loaded customer file: %s (%d companies)", csv_file.name, len(names))
            except ValueError as e:
                logger.warning("Skipping %s: %s", csv_file.name, e)
            except Exception:
                logger.exception("Error loading customer CSV: %s", csv_file.name)

        if not customer_names_raw:
            logger.warning("No customer company names extracted")
            return set()

        customer_lookup = {_normalize_company_name(name) for name in customer_names_raw}
        logger.info("Customer lookup: %d unique normalized names", len(customer_lookup))
        return customer_lookup

    # ── Step 7: Match Target Accounts ────────────────────────────────────

    def match_target_accounts(self, master_df, account_name_col=None):
        """
        Match connections' current_company against target account lists.

        Adds columns: is_target_account, target_account_name
        """
        accounts_dir = self.assets_dir / "Accounts"
        if not accounts_dir.exists():
            logger.warning("Accounts directory not found at %s", accounts_dir)
            master_df["is_target_account"] = False
            master_df["target_account_name"] = ""
            return master_df

        # Load all account CSVs
        csv_files = list(accounts_dir.glob("*.csv"))
        if not csv_files:
            logger.warning("No CSV files found in %s", accounts_dir)
            master_df["is_target_account"] = False
            master_df["target_account_name"] = ""
            return master_df

        all_accounts = []
        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file, encoding="utf-8", low_memory=False)
                all_accounts.append(df)
                logger.info("Loaded account file: %s (%d rows)", csv_file.name, len(df))
            except Exception:
                logger.exception("Error loading account CSV: %s", csv_file.name)

        if not all_accounts:
            master_df["is_target_account"] = False
            master_df["target_account_name"] = ""
            return master_df

        accounts_df = pd.concat(all_accounts, ignore_index=True)

        # Detect company name column
        name_col = account_name_col or _detect_column(
            accounts_df, _ACCOUNT_NAME_CANDIDATES, "account name"
        )

        # Build normalized name → original name lookup
        account_lookup = {}
        for name in accounts_df[name_col].dropna().unique():
            normalized = _normalize_company_name(str(name))
            account_lookup[normalized] = str(name)

        # Match against master_df.current_company
        if "current_company" not in master_df.columns:
            logger.warning(
                "current_company column missing — run enrich_for_matching first. "
                "Falling back to company field."
            )
            company_col = "company" if "company" in master_df.columns else None
        else:
            company_col = "current_company"

        if company_col is None:
            logger.warning("No company column available for account matching")
            master_df["is_target_account"] = False
            master_df["target_account_name"] = ""
            return master_df

        def _match_account(company):
            if pd.isna(company) or not company:
                return False, ""
            normalized = _normalize_company_name(str(company))
            if normalized in account_lookup:
                return True, account_lookup[normalized]
            return False, ""

        matches = master_df[company_col].apply(_match_account)
        master_df["is_target_account"] = matches.apply(lambda x: x[0])
        master_df["target_account_name"] = matches.apply(lambda x: x[1])

        logger.info(
            "Account matching complete: %d connections at target accounts",
            master_df["is_target_account"].sum(),
        )
        return master_df

    # ── Step 7: Match Target Prospects ───────────────────────────────────

    def match_target_prospects(self, master_df, prospect_id_col=None):
        """
        Match connections against target prospect lists by linkedin_profile_id.

        Adds columns: is_target_prospect, prospect_icp_tag
        """
        prospects_dir = self.assets_dir / "Prospects"
        if not prospects_dir.exists():
            logger.warning("Prospects directory not found at %s", prospects_dir)
            master_df["is_target_prospect"] = False
            master_df["prospect_icp_tag"] = ""
            return master_df

        csv_files = list(prospects_dir.glob("*.csv"))
        if not csv_files:
            logger.warning("No CSV files found in %s", prospects_dir)
            master_df["is_target_prospect"] = False
            master_df["prospect_icp_tag"] = ""
            return master_df

        all_prospects = []
        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file, encoding="utf-8", low_memory=False)
                all_prospects.append(df)
                logger.info("Loaded prospect file: %s (%d rows)", csv_file.name, len(df))
            except Exception:
                logger.exception("Error loading prospect CSV: %s", csv_file.name)

        if not all_prospects:
            master_df["is_target_prospect"] = False
            master_df["prospect_icp_tag"] = ""
            return master_df

        prospects_df = pd.concat(all_prospects, ignore_index=True)

        # Detect profile ID column
        id_col = prospect_id_col or _detect_column(
            prospects_df, _PROSPECT_ID_CANDIDATES, "prospect LinkedIn ID"
        )

        # Coerce to int and build lookup: pid → icp_tag
        prospects_df["_pid"] = pd.to_numeric(
            prospects_df[id_col], errors="coerce"
        ).astype("Int64")

        # Detect ICP tag column
        icp_col = None
        for candidate in _PROSPECT_ICP_CANDIDATES:
            matches = [
                c for c in prospects_df.columns
                if c.lower().strip() == candidate.lower().strip()
            ]
            if matches:
                icp_col = matches[0]
                break

        # Build lookup
        prospect_lookup = {}
        for _, row in prospects_df.iterrows():
            pid = row.get("_pid")
            if pd.isna(pid):
                continue
            pid = int(pid)
            icp_tag = ""
            if icp_col:
                tag_val = row.get(icp_col)
                icp_tag = str(tag_val) if not pd.isna(tag_val) else ""
            prospect_lookup[pid] = icp_tag

        logger.info(
            "Prospect lookup built: %d unique profiles (ICP col: %s)",
            len(prospect_lookup), icp_col or "none found",
        )

        # Match against master
        def _match_prospect(pid):
            if pd.isna(pid):
                return False, ""
            pid_int = int(pid)
            if pid_int in prospect_lookup:
                return True, prospect_lookup[pid_int]
            return False, ""

        matches = master_df["linkedin_profile_id"].apply(_match_prospect)
        master_df["is_target_prospect"] = matches.apply(lambda x: x[0])
        master_df["prospect_icp_tag"] = matches.apply(lambda x: x[1])

        logger.info(
            "Prospect matching complete: %d connections are target prospects",
            master_df["is_target_prospect"].sum(),
        )
        return master_df


# ── Module-level helpers ─────────────────────────────────────────────────────

def _extract_handle_from_url(url: str) -> str:
    """
    Extract the LinkedIn profile handle from a URL.
    E.g., 'https://www.linkedin.com/in/johndoe/' → 'johndoe'
          'https://www.linkedin.com/posts/johndoe_...' → 'johndoe'
    """
    if not url:
        return ""
    # Match /in/handle or /posts/handle_ patterns
    match = re.search(r"linkedin\.com/(?:in|posts)/([^/?_]+)", url)
    if match:
        return match.group(1).strip("/")
    return ""


def _match_customer_name(company, customer_lookup):
    """Check if a company name matches any customer (exact or starts-with)."""
    if pd.isna(company) if hasattr(pd, 'isna') else company is None:
        return False
    company = str(company).strip()
    if not company:
        return False
    norm = _normalize_company_name(company)
    # Exact match
    if norm in customer_lookup:
        return True
    # "Starts with" match: catches "Agicap France" → "agicap",
    # "ZELIQ (ex-GetHeroes)" → "zeliq"
    for cust in customer_lookup:
        if norm.startswith(cust + " ") or norm.startswith(cust + "("):
            return True
    return False


def _normalize_company_name(name: str) -> str:
    """Normalize a company name for fuzzy matching: lowercase, strip suffixes and punctuation."""
    name = name.strip().lower()
    name = _COMPANY_SUFFIXES.sub("", name).strip()
    # Remove trailing punctuation and extra whitespace
    name = re.sub(r"[.,;:!?]+$", "", name).strip()
    name = re.sub(r"\s+", " ", name)
    return name


def _parse_date(date_str: str):
    """
    Parse a date string into a (year, month) tuple.
    Handles formats like 'Jan 2020', '2020-01', '2020', 'Present'.
    Returns None if unparseable.
    """
    if not date_str:
        return None

    date_str = str(date_str).strip()

    if date_str.lower() in ("present", "current", "now"):
        return (9999, 12)  # Sentinel for "still active"

    # Try YYYY-MM
    match = re.match(r"(\d{4})-(\d{1,2})", date_str)
    if match:
        return (int(match.group(1)), int(match.group(2)))

    # Try "Mon YYYY" (e.g., "Jan 2020")
    month_names = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }
    match = re.match(r"([a-zA-Z]{3})\w*\s+(\d{4})", date_str)
    if match:
        month = month_names.get(match.group(1).lower()[:3])
        if month:
            return (int(match.group(2)), month)

    # Try bare YYYY
    match = re.match(r"^(\d{4})$", date_str)
    if match:
        return (int(match.group(1)), 1)

    return None


def _dates_overlap(start_a, end_a, start_b, end_b):
    """
    Check if two date ranges overlap by at least 1 month.
    Dates are (year, month) tuples or None.
    None start = distant past; None end = present.
    """
    # Default missing bounds
    s_a = start_a or (1900, 1)
    e_a = end_a or (9999, 12)
    s_b = start_b or (1900, 1)
    e_b = end_b or (9999, 12)

    # Convert to comparable months-since-epoch
    def to_months(ym):
        return ym[0] * 12 + ym[1]

    sa_m, ea_m = to_months(s_a), to_months(e_a)
    sb_m, eb_m = to_months(s_b), to_months(e_b)

    # Overlap exists if ranges intersect by >= 1 month
    overlap = min(ea_m, eb_m) - max(sa_m, sb_m)
    return overlap >= 1


def _detect_column(df, candidates, description):
    """
    Auto-detect a column from a DataFrame by trying candidate names (case-insensitive).
    Raises ValueError if none found.
    """
    col_map = {c.lower().strip(): c for c in df.columns}
    for candidate in candidates:
        key = candidate.lower().strip()
        if key in col_map:
            logger.info("Auto-detected %s column: '%s'", description, col_map[key])
            return col_map[key]

    raise ValueError(
        f"Could not auto-detect {description} column. "
        f"Tried: {candidates}. "
        f"Available columns: {list(df.columns)}"
    )
