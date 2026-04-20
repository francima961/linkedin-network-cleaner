"""
Extraction functions built on top of the generic EdgesClient.
Implements the 11 extraction tasks needed for audience analysis,
with checkpointing and chain methods for long-running operations.
"""

import csv
import json
import glob as globmod
import logging
from datetime import datetime, timezone
from pathlib import Path

from .edges_client import EdgesClient
from .session_logger import log_session_event
from . import config

logger = logging.getLogger(__name__)


class AudienceExtractor:
    """Extracts all audience data from a LinkedIn account via Edges API."""

    def __init__(self, client: EdgesClient):
        self.client = client
        config.ensure_dirs()

    # ── Individual Extractors ─────────────────────────────────────────────

    def extract_connections(self, progress_callback=None, max_results=None):
        """Extract all connections. Direct mode, no input needed."""
        results, meta = self.client.paginated_call(
            "extract-connections",
            direct_mode=True,
            dedup_key="linkedin_profile_id",
            progress_callback=progress_callback,
            max_results=max_results,
        )
        self._save_extract("connections", results, meta)
        return results, meta

    def extract_conversations(self, progress_callback=None, max_results=None):
        """Extract all conversations. Direct mode, no input needed."""
        results, meta = self.client.paginated_call(
            "extract-conversations",
            direct_mode=True,
            dedup_key="linkedin_thread_id",
            progress_callback=progress_callback,
            max_results=max_results,
        )
        self._save_extract("conversations", results, meta)
        return results, meta

    def extract_messages(self, thread_url):
        """Extract messages from a single conversation thread."""
        results, meta = self.client.paginated_call(
            "extract-messages",
            input_data={"linkedin_thread_url": thread_url},
            direct_mode=True,
            dedup_key="message_id",
            max_empty=2,
        )
        return results, meta

    def extract_followers(self, progress_callback=None, max_results=None):
        """Extract all followers. Direct mode, no input needed."""
        results, meta = self.client.paginated_call(
            "extract-followers",
            direct_mode=True,
            dedup_key="linkedin_profile_id",
            progress_callback=progress_callback,
            max_results=max_results,
        )
        self._save_extract("followers", results, meta)
        return results, meta

    def extract_profile_viewers(self, progress_callback=None, max_results=None):
        """Extract profile viewers. Direct mode, no input needed."""
        results, meta = self.client.paginated_call(
            "extract-profile-viewers",
            direct_mode=True,
            dedup_key="linkedin_profile_id",
            progress_callback=progress_callback,
            max_results=max_results,
        )
        self._save_extract("profile_viewers", results, meta)
        return results, meta

    def extract_posts(self, profile_url, progress_callback=None, max_results=None):
        """Extract all posts for a profile. cursor_only because page-number
        fallback loops infinitely for this endpoint (API re-serves same posts)."""
        results, meta = self.client.paginated_call(
            "extract-people-post-activity",
            input_data={"url": profile_url},
            direct_mode=False,
            dedup_key="linkedin_post_id",
            cursor_only=True,
            progress_callback=progress_callback,
            max_results=max_results,
        )
        self._save_extract("posts", results, meta)
        return results, meta

    def extract_post_likers(self, post_url):
        """Extract all likers of a post. page_size=30 required in URL."""
        results, meta = self.client.paginated_call(
            "extract-post-likers",
            input_data={"linkedin_post_url": post_url},
            direct_mode=False,
            query_params={"page_size": "30"},
            dedup_key="linkedin_profile_id",
            max_empty=2,
        )
        return results, meta

    def extract_post_commenters(self, post_url):
        """Extract all commenters on a post. page_size=10, sort_order=newest."""
        results, meta = self.client.paginated_call(
            "extract-post-commenters",
            input_data={"linkedin_post_url": post_url},
            direct_mode=False,
            parameters={"sort_order": "newest"},
            query_params={"page_size": "10"},
            dedup_key="linkedin_comment_id",
            max_empty=5,
        )
        return results, meta

    def extract_post_reposters(self, post_url):
        """Extract all reposters of a post. Direct mode."""
        results, meta = self.client.paginated_call(
            "extract-post-reposters",
            input_data={"linkedin_post_url": post_url},
            direct_mode=True,
            dedup_key="linkedin_profile_id",
            max_empty=2,
        )
        return results, meta

    def extract_reaction_activity(self, profile_url, progress_callback=None, max_results=None):
        """Extract reaction activity for a profile."""
        results, meta = self.client.paginated_call(
            "extract-people-reaction-activity",
            input_data={"linkedin_profile_url": profile_url},
            direct_mode=False,
            dedup_key="linkedin_post_id",
            progress_callback=progress_callback,
            max_results=max_results,
        )
        self._save_extract("reaction_activity", results, meta)
        return results, meta

    def extract_comment_activity(self, profile_url, progress_callback=None, max_results=None):
        """Extract comment activity for a profile."""
        results, meta = self.client.paginated_call(
            "extract-people-comment-activity",
            input_data={"linkedin_profile_url": profile_url},
            direct_mode=False,
            dedup_key="linkedin_post_id",
            progress_callback=progress_callback,
            max_results=max_results,
        )
        self._save_extract("comment_activity", results, meta)
        return results, meta

    def extract_sent_invitations(self, progress_callback=None, max_results=None):
        """Extract all pending sent invitations. Direct mode, no input needed."""
        results, meta = self.client.paginated_call(
            "extract-sent-invitations",
            direct_mode=True,
            dedup_key="linkedin_invitation_id",
            query_params={"page_size": "100"},
            progress_callback=progress_callback,
            max_results=max_results,
        )
        self._save_extract("sent_invitations", results, meta)
        return results, meta

    # ── Chain Methods (with checkpointing) ────────────────────────────────

    def extract_all_messages(self, conversations=None, resume=False, progress_callback=None, max_items=None):
        """
        Extract messages from ALL conversation threads.
        Checkpoints every 50 threads. On LIMIT_REACHED or crash: saves progress.
        """
        if conversations is None:
            conversations = self._load_latest_extract("conversations")
            if conversations is None:
                logger.error("No conversations data found. Run --conversations first.")
                return None, {"error": "no_conversations_data"}

        # Resume from checkpoint if requested
        start_index = 0
        all_messages = {}
        if resume:
            checkpoint = self._load_latest_checkpoint("messages")
            if checkpoint:
                all_messages = checkpoint.get("data", {})
                start_index = checkpoint.get("index", 0)
                logger.info("Resuming messages from thread index %d (%d threads already done)",
                            start_index, len(all_messages))

        errors = []
        limit_reached = False
        checkpoint_interval = 50

        end_index = len(conversations)
        if max_items:
            end_index = min(start_index + max_items, len(conversations))

        for i in range(start_index, end_index):
            conv = conversations[i]
            thread_url = conv.get("linkedin_thread_url") or conv.get("linkedin_thread_id")
            if not thread_url:
                logger.warning("Thread %d: no URL/ID — skipping", i)
                continue

            logger.info("Thread %d/%d: %s", i + 1, len(conversations), thread_url)
            msgs, meta = self.extract_messages(thread_url)

            if meta.get("limit_reached"):
                limit_reached = True
                logger.warning("LIMIT_REACHED at thread %d — saving progress", i)
                self._save_checkpoint("messages", all_messages, i)
                break

            if meta.get("errors"):
                errors.extend(meta["errors"])

            if msgs:
                thread_id = conv.get("linkedin_thread_id", thread_url)
                all_messages[thread_id] = msgs

            # Progress callback: report thread-level progress
            if progress_callback:
                total_msgs = sum(len(m) for m in all_messages.values())
                progress_callback(i + 1, len(conversations), total_msgs)

            # Checkpoint every N threads
            if (i + 1 - start_index) % checkpoint_interval == 0:
                self._save_checkpoint("messages", all_messages, i + 1)
                logger.info("Checkpoint saved at thread %d", i + 1)

        # Final save
        flat_messages = []
        for thread_msgs in all_messages.values():
            flat_messages.extend(thread_msgs)

        meta = {
            "threads_processed": len(all_messages),
            "total_messages": len(flat_messages),
            "errors": errors,
            "limit_reached": limit_reached,
        }
        self._save_extract("messages", flat_messages, meta)

        # Also save the per-thread structure
        self._save_extract("messages_by_thread", all_messages, meta)

        return flat_messages, meta

    def extract_all_post_engagement(self, posts=None, resume=False, progress_callback=None, max_items=None):
        """
        Extract likers + commenters + reposters for ALL posts.
        Checkpoints every 10 posts. On LIMIT_REACHED or crash: saves progress.
        """
        if posts is None:
            posts = self._load_latest_extract("posts")
            if posts is None:
                logger.error("No posts data found. Run --posts first.")
                return None, {"error": "no_posts_data"}

        # Resume from checkpoint if requested
        start_index = 0
        engagement = {"likers": {}, "commenters": {}, "reposters": {}}
        if resume:
            checkpoint = self._load_latest_checkpoint("post_engagement")
            if checkpoint:
                engagement = checkpoint.get("data", engagement)
                start_index = checkpoint.get("index", 0)
                logger.info("Resuming post engagement from index %d (%d posts already done)",
                            start_index, len(engagement["likers"]))

        errors = []
        limit_reached = False
        checkpoint_interval = 10

        end_index = len(posts)
        if max_items:
            end_index = min(start_index + max_items, len(posts))

        for i in range(start_index, end_index):
            post = posts[i]
            post_url = post.get("linkedin_post_url") or post.get("post_url")
            post_id = post.get("linkedin_post_id", str(i))
            if not post_url:
                logger.warning("Post %d: no URL — skipping", i)
                continue

            logger.info("Post %d/%d: %s", i + 1, len(posts), post_url)

            # Likers
            likers, liker_meta = self.extract_post_likers(post_url)
            if liker_meta.get("limit_reached"):
                limit_reached = True
                logger.warning("LIMIT_REACHED (likers) at post %d — saving", i)
                self._save_checkpoint("post_engagement", engagement, i)
                break
            engagement["likers"][post_id] = likers or []

            # Commenters
            commenters, commenter_meta = self.extract_post_commenters(post_url)
            if commenter_meta.get("limit_reached"):
                limit_reached = True
                logger.warning("LIMIT_REACHED (commenters) at post %d — saving", i)
                self._save_checkpoint("post_engagement", engagement, i)
                break
            engagement["commenters"][post_id] = commenters or []

            # Reposters
            reposters, reposter_meta = self.extract_post_reposters(post_url)
            if reposter_meta.get("limit_reached"):
                limit_reached = True
                logger.warning("LIMIT_REACHED (reposters) at post %d — saving", i)
                self._save_checkpoint("post_engagement", engagement, i)
                break
            engagement["reposters"][post_id] = reposters or []

            if liker_meta.get("errors"):
                errors.extend(liker_meta["errors"])
            if commenter_meta.get("errors"):
                errors.extend(commenter_meta["errors"])
            if reposter_meta.get("errors"):
                errors.extend(reposter_meta["errors"])

            # Progress callback: report post-level progress
            if progress_callback:
                total_eng = (
                    sum(len(v) for v in engagement["likers"].values())
                    + sum(len(v) for v in engagement["commenters"].values())
                    + sum(len(v) for v in engagement["reposters"].values())
                )
                progress_callback(i + 1, len(posts), total_eng)

            # Checkpoint every N posts
            if (i + 1 - start_index) % checkpoint_interval == 0:
                self._save_checkpoint("post_engagement", engagement, i + 1)
                logger.info("Checkpoint saved at post %d", i + 1)

        # Final saves — flatten each type
        all_likers = [p for likers in engagement["likers"].values() for p in likers]
        all_commenters = [c for comms in engagement["commenters"].values() for c in comms]
        all_reposters = [r for reps in engagement["reposters"].values() for r in reps]

        meta = {
            "posts_processed": len(engagement["likers"]),
            "total_likers": len(all_likers),
            "total_commenters": len(all_commenters),
            "total_reposters": len(all_reposters),
            "errors": errors,
            "limit_reached": limit_reached,
        }

        self._save_extract("post_likers", all_likers, meta)
        self._save_extract("post_commenters", all_commenters, meta)
        self._save_extract("post_reposters", all_reposters, meta)
        self._save_extract("post_engagement_by_post", engagement, meta)

        return engagement, meta

    # ── Persistence Helpers ───────────────────────────────────────────────

    def _save_extract(self, name, data, meta):
        """Save extraction results to extracts/ as JSON + CSV, and log the action."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        json_filename = f"{name}_{ts}.json"
        json_filepath = config.EXTRACTS_DIR / json_filename

        payload = {
            "extract_name": name,
            "timestamp": ts,
            "record_count": len(data) if isinstance(data, (list, dict)) else 0,
            "metadata": meta,
            "data": data,
        }

        json_filepath.write_text(json.dumps(payload, default=str, ensure_ascii=False), encoding="utf-8")
        logger.info("Saved %s → %s (%s records)", name, json_filename,
                     len(data) if isinstance(data, (list, dict)) else "?")

        # CSV export — only for flat list-of-dicts data
        csv_filename = None
        if isinstance(data, list) and data and isinstance(data[0], dict):
            csv_filename = f"{name}_{ts}.csv"
            csv_filepath = config.EXTRACTS_DIR / csv_filename
            # Collect all keys across all records for complete headers
            fieldnames = list(dict.fromkeys(k for row in data for k in row.keys()))
            with open(csv_filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(data)
            logger.info("Saved %s → %s", name, csv_filename)

        # Action log
        log_entry = {
            "action": "extract",
            "extract_name": name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "json_file": json_filename,
            "csv_file": csv_filename,
            "record_count": len(data) if isinstance(data, (list, dict)) else 0,
            "metadata": meta,
        }
        log_file = config.ACTIONS_LOG_DIR / f"extract_{name}_{ts}.json"
        log_file.write_text(json.dumps(log_entry, default=str, ensure_ascii=False), encoding="utf-8")

        # Session log for cross-session handoff
        record_count = len(data) if isinstance(data, (list, dict)) else 0
        log_session_event("extraction", f"Extracted {name}: {record_count} records", {
            "file": json_filename,
        })

    def _load_latest_extract(self, name):
        """Load the most recent extract by name. Returns the data field or None."""
        pattern = str(config.EXTRACTS_DIR / f"{name}_*.json")
        files = sorted(globmod.glob(pattern))
        if not files:
            return None

        latest = files[-1]
        logger.info("Loading latest %s extract: %s", name, Path(latest).name)
        with open(latest, encoding="utf-8") as f:
            payload = json.load(f)
        return payload.get("data")

    def _save_checkpoint(self, name, data, index):
        """Save intermediate progress for chain methods."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"{name}_checkpoint_{ts}.json"
        filepath = config.EXTRACTS_DIR / filename

        payload = {
            "checkpoint_name": name,
            "timestamp": ts,
            "index": index,
            "data": data,
        }

        filepath.write_text(json.dumps(payload, default=str, ensure_ascii=False), encoding="utf-8")
        logger.info("Checkpoint %s saved at index %d → %s", name, index, filename)

    def _load_latest_checkpoint(self, name):
        """Load the most recent checkpoint. Returns dict with data and index, or None."""
        pattern = str(config.EXTRACTS_DIR / f"{name}_checkpoint_*.json")
        files = sorted(globmod.glob(pattern))
        if not files:
            return None

        latest = files[-1]
        logger.info("Loading checkpoint: %s", Path(latest).name)
        with open(latest, encoding="utf-8") as f:
            return json.load(f)
