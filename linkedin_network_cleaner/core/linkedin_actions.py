"""
LinkedIn direct actions: withdraw invitations, unfollow profiles.

- withdraw_invite: uses Edges API native skill (linkedin-withdraw-invitation)
- unfollow: uses Edges API native skill (linkedin-follow-profile with unfollow=true)
- remove_connection: NOT YET AVAILABLE — requires LinkedIn internal API (coming soon)

All actions default to dry_run=True and log every operation to logs/actions/ and logs/data/.
"""

import json
import logging
from datetime import datetime, timezone

from . import config
from .edges_client import EdgesClient

logger = logging.getLogger(__name__)


class LinkedInActions:
    """Execute network-altering LinkedIn actions with mandatory logging."""

    def __init__(self, edges_client: EdgesClient = None):
        """
        Initialize actions handler.

        Args:
            edges_client: EdgesClient instance for API-based actions.
                          If None, a new one is created from config.
        """
        self.edges_client = edges_client or EdgesClient(
            api_key=config.API_KEY,
            identity_uuid=config.IDENTITY_UUID,
        )
        config.ensure_dirs()

    # ── Remove Connection (Coming Soon) ──────────────────────────────────

    def remove_connection(self, vanity_name, profile_data, dry_run=True):
        """
        Remove a LinkedIn connection.

        NOTE: Connection removal is not yet available in the public release.
        It requires LinkedIn's internal browser API which depends on session
        cookies that expire and are difficult to configure reliably.

        This will be enabled once a stable API method is available.
        """
        logger.warning("Connection removal is not yet available in this version.")
        return {
            "success": False,
            "action": "remove_connection",
            "vanity_name": vanity_name,
            "full_name": profile_data.get("full_name", "") if profile_data else "",
            "dry_run": dry_run,
            "error": "Connection removal is coming soon. "
                     "Currently only invite withdrawal and unfollow are supported.",
        }

    # ── Withdraw Invitation (Edges API) ───────────────────────────────────

    def withdraw_invite(self, invite_record, dry_run=True):
        """
        Withdraw a pending sent invitation via the Edges API.

        Args:
            invite_record: Full invitation dict from extract-sent-invitations.
                          Must contain linkedin_invitation_urn.
            dry_run: If True, only log what would happen. Default True.

        Returns:
            dict with keys: success, action, invitation_id, dry_run, error (if any)
        """
        if not invite_record:
            return {"success": False, "action": "withdraw_invite",
                    "error": "invite_record is required"}

        invitation_urn = invite_record.get("linkedin_invitation_urn")
        invitation_id = invite_record.get("linkedin_invitation_id")

        if not invitation_urn:
            return {"success": False, "action": "withdraw_invite",
                    "error": "linkedin_invitation_urn missing from invite_record"}

        result = {
            "action": "withdraw_invite",
            "linkedin_invitation_id": invitation_id,
            "linkedin_invitation_urn": invitation_urn,
            "full_name": invite_record.get("full_name", ""),
            "dry_run": dry_run,
        }

        # Always log the data snapshot
        self._log_data("withdraw_invite", invite_record)

        if dry_run:
            result["success"] = True
            result["message"] = "DRY RUN — would withdraw invitation"
            logger.info("[DRY RUN] Would withdraw invite to: %s (%s)",
                        invite_record.get("full_name", ""), invitation_id)
            self._log_action("withdraw_invite", result)
            return result

        # Execute via Edges API
        data, _headers, error = self.edges_client.call_action(
            "withdraw-invitation",
            input_data={
                "linkedin_invitation_urn": invitation_urn,
                "linkedin_invitation_type": "CONNECTION",
            },
            direct_mode=True,
        )

        if error is not None:
            result["success"] = False
            result["error"] = error
            logger.error("Failed to withdraw invite %s: %s",
                         invitation_id, error.get("error_label", "UNKNOWN"))
        else:
            result["success"] = True
            result["message"] = "Invitation withdrawn"
            logger.info("Withdrawn invite to: %s (%s)",
                        invite_record.get("full_name", ""), invitation_id)

        self._log_action("withdraw_invite", result)
        return result

    # ── Unfollow (Edges API) ─────────────────────────────────────────────

    def unfollow(self, profile_data, dry_run=True):
        """
        Unfollow a LinkedIn profile via the Edges API (follow-profile with unfollow=true).

        Args:
            profile_data: Profile dict — must contain linkedin_profile_url.
                          Backed up before action (mandatory).
            dry_run: If True, only log what would happen. Default True.

        Returns:
            dict with keys: success, action, dry_run, error (if any)
        """
        if not profile_data:
            return {"success": False, "action": "unfollow",
                    "error": "profile_data is required"}

        profile_url = profile_data.get("linkedin_profile_url")
        if not profile_url:
            return {"success": False, "action": "unfollow",
                    "error": "linkedin_profile_url missing from profile_data"}

        result = {
            "action": "unfollow",
            "linkedin_profile_url": profile_url,
            "linkedin_profile_id": profile_data.get("linkedin_profile_id"),
            "full_name": profile_data.get("full_name", ""),
            "dry_run": dry_run,
        }

        # Always log the data snapshot
        self._log_data("unfollow", profile_data)

        if dry_run:
            result["success"] = True
            result["message"] = "DRY RUN — would unfollow"
            logger.info("[DRY RUN] Would unfollow: %s (%s)",
                        profile_data.get("full_name", ""), profile_url)
            self._log_action("unfollow", result)
            return result

        # Execute via Edges API (follow-profile with unfollow parameter)
        data, _headers, error = self.edges_client.call_action(
            "follow-profile",
            input_data={"linkedin_profile_url": profile_url},
            direct_mode=True,
            parameters={"unfollow": True},
        )

        if error is not None:
            result["success"] = False
            result["error"] = error
            logger.error("Failed to unfollow %s: %s",
                         profile_data.get("full_name", ""),
                         error.get("error_label", "UNKNOWN"))
        else:
            result["success"] = True
            result["followed"] = data.get("followed") if isinstance(data, dict) else None
            result["message"] = "Unfollowed"
            logger.info("Unfollowed: %s (%s)",
                        profile_data.get("full_name", ""), profile_url)

        self._log_action("unfollow", result)
        return result

    # ── Logging ───────────────────────────────────────────────────────────

    def _log_action(self, action_type, result):
        """Log action to logs/actions/ — what happened, when."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **result,
        }
        log_file = config.ACTIONS_LOG_DIR / f"{action_type}_{ts}.json"
        log_file.write_text(
            json.dumps(log_entry, default=str, ensure_ascii=False),
            encoding="utf-8",
        )

    def _log_data(self, action_type, data):
        """Log full data snapshot to logs/data/ — for rollback safety."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        snapshot = {
            "action": action_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": data,
        }
        log_file = config.DATA_LOG_DIR / f"{action_type}_{ts}.json"
        log_file.write_text(
            json.dumps(snapshot, default=str, ensure_ascii=False),
            encoding="utf-8",
        )
