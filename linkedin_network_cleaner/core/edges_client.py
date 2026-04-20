"""
Generic Edges API client — reusable by any script (extraction, cleaning, outreach, enrichment).
Knows nothing about audience cleaning. Pure API interaction layer.
"""

import time
import logging
import requests

logger = logging.getLogger(__name__)


def compute_max_workers(credits, num_profiles):
    """Compute optimal concurrent workers from plan credits and profile count."""
    # Tier → max workers (with 35% headroom from plan limit)
    if credits <= 1_000:        tier_max = 1
    elif credits <= 10_000:     tier_max = 1
    elif credits <= 100_000:    tier_max = 5
    elif credits <= 2_000_000:  tier_max = 8
    elif credits <= 20_000_000: tier_max = 8
    else:                       tier_max = 15

    # Scale down for small profile counts
    if num_profiles <= 200:
        return min(tier_max, 1)
    elif num_profiles <= 2_000:
        return min(tier_max, 3)
    return tier_max


# Errors that should NEVER be retried
NO_RETRY_ERRORS = frozenset({
    "BAD_INPUT", "BAD_PARAMETERS", "MISSING_PARAMETER",
    "NO_ACCESS", "STATUS_403", "STATUS_404", "NO_RESULT",
    "NOT_CONNECTED", "ALREADY_CONNECTED", "INVITATION_PENDING",
    "PROFILE_NOT_ACCESSIBLE", "AUTH_EXPIRED",
    "SN_CONFLICT", "SN_OUT_OF_NETWORK",
    "NO_VALID_ACCOUNT_CONFIGURED", "INTEGRATION_ERROR", "PROXY_ERROR",
    "MANDATORY_DATA_MISSING", "UNDEFINED_FIELD", "UNKNOWN_ERROR", "ACTION_ABORTED",
    "LIMIT_REACHED",
    "LK_INMAIL_CANNOT_RESEND", "LK_INMAIL_NOT_ENOUGH_CREDIT", "LK_EVENT",
})

# Errors that CAN be retried with exponential backoff
RETRY_ERRORS = frozenset({
    "STATUS_429", "LK_ERROR", "LK_524", "GENERIC_ERROR", "NO_DATA_LOADED",
})


class EdgesClient:
    """Generic Edges API client with retry logic, pagination, and async support."""

    def __init__(self, api_key, identity_uuid, base_url="https://api.edges.run/v1", delay=1.5):
        self.api_key = api_key
        self.identity_uuid = identity_uuid
        self.base_url = base_url.rstrip("/")
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            "X-API-Key": self.api_key,
            "Content-Type": "application/json",
        })

    # ── Public API ────────────────────────────────────────────────────────

    def get_workspace_info(self):
        """Fetch workspace info including credits. Returns dict or None."""
        url = f"{self.base_url}/workspaces"
        data, _, error = self._request_with_retry("GET", url)
        if error is not None:
            logger.warning("Failed to fetch workspace info: %s", error)
            return None
        return data

    # ── Identity Management (no instance needed) ─────────────────────────

    @staticmethod
    def list_identities(api_key, base_url="https://api.edges.run/v1"):
        """List all identities for the API key. Returns list of identity dicts or raises."""
        resp = requests.get(
            f"{base_url}/identities?retrieve_accounts=true",
            headers={"X-API-Key": api_key, "Content-Type": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def create_identity(api_key, name, timezone="UTC", base_url="https://api.edges.run/v1"):
        """Create a new identity. Returns identity dict with uid + login links."""
        resp = requests.post(
            f"{base_url}/identities",
            headers={"X-API-Key": api_key, "Content-Type": "application/json"},
            json={"name": name, "timezone": timezone},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def generate_login_link(api_key, identity_uid, base_url="https://api.edges.run/v1"):
        """Generate a fresh LinkedIn login link for an existing identity."""
        resp = requests.post(
            f"{base_url}/identities/{identity_uid}/generate-login-links",
            headers={"X-API-Key": api_key, "Content-Type": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def call_action(self, action_slug, input_data=None, direct_mode=False,
                    parameters=None, query_params=None):
        """
        Execute a single Edges action (any skill) in live mode.
        Returns (data, headers, error_info).
        - data: parsed JSON response (list or dict), or None on total failure
        - headers: response headers dict
        - error_info: dict with error details if the call failed, None on success
        """
        url = f"{self.base_url}/actions/linkedin-{action_slug}/run/live"
        if query_params:
            qs = "&".join(f"{k}={v}" for k, v in query_params.items())
            url = f"{url}?{qs}"

        body = self._build_body(input_data, direct_mode, parameters, is_async=False)
        return self._request_with_retry("POST", url, body=body)

    def paginated_call(self, action_slug, input_data=None, direct_mode=False,
                       parameters=None, query_params=None, dedup_key=None,
                       max_pages=10000, max_empty=20, cursor_only=False,
                       max_results=None, progress_callback=None):
        """
        Call an action with pagination, collecting all pages.
        Uses cursor-based pagination (X-Pagination-Next header) first.
        Falls back to page-number pagination (&page=N) when cursors stop early.
        Set cursor_only=True to disable page-number fallback (for endpoints
        where &page=N loops infinitely, e.g. extract-people-post-activity).
        Returns (all_results, metadata).
        """
        base_url = f"{self.base_url}/actions/linkedin-{action_slug}/run/live"
        if query_params:
            qs = "&".join(f"{k}={v}" for k, v in query_params.items())
            base_url = f"{base_url}?{qs}"

        body = self._build_body(input_data, direct_mode, parameters, is_async=False)

        all_results = []
        errors = []
        page = 0
        limit_reached = False
        current_url = base_url
        use_page_numbers = False
        consecutive_empty = 0

        while page < max_pages:
            page += 1

            if use_page_numbers:
                sep = "&" if "?" in base_url else "?"
                current_url = f"{base_url}{sep}page={page}"

            data, headers, error = self._request_with_retry("POST", current_url, body=body)

            if error is not None:
                errors.append({"page": page, "error": error})
                if error.get("error_label") == "LIMIT_REACHED":
                    limit_reached = True
                    logger.warning("LIMIT_REACHED on page %d — stopping pagination", page)
                break

            if data is None:
                errors.append({"page": page, "error": {"error_label": "NULL_RESPONSE"}})
                break

            # data should be a list for paginated endpoints
            if isinstance(data, list):
                if len(data) == 0:
                    if use_page_numbers:
                        consecutive_empty += 1
                        if consecutive_empty >= max_empty:
                            logger.info("%d consecutive empty pages — pagination complete",
                                        max_empty)
                            break
                    else:
                        if cursor_only:
                            logger.info("Cursor pagination returned empty at page %d — "
                                        "cursor_only mode, stopping", page)
                            break
                        # Cursor pagination returned empty — switch to page numbers
                        # Start from page 1 (page numbering is independent of cursor)
                        # Dedup key will handle any overlap with cursor results
                        # API needs a brief cooldown after cursor exhaustion
                        logger.info("Cursor pagination returned empty at page %d — "
                                    "switching to page-number pagination from page 1", page)
                        use_page_numbers = True
                        consecutive_empty = 0
                        page = 0  # will become 1 after page += 1 at loop top
                        time.sleep(15)
                        continue
                else:
                    consecutive_empty = 0
                    all_results.extend(data)
                    logger.info("Page %d: %d results (total: %d)", page, len(data), len(all_results))
            elif isinstance(data, dict):
                all_results.append(data)
                logger.info("Page %d: 1 result (dict)", page)

            # Progress callback
            if progress_callback:
                page_count = len(data) if isinstance(data, list) else 1
                progress_callback(page, page_count, len(all_results))

            # Max results cap
            if max_results and len(all_results) >= max_results:
                all_results = all_results[:max_results]
                logger.info("Reached max_results=%d — stopping pagination", max_results)
                break

            if not use_page_numbers:
                # Check for next page cursor
                current_url = None
                if headers:
                    next_cursor = headers.get("X-Pagination-Next") or headers.get("x-pagination-next")
                    if next_cursor:
                        current_url = next_cursor
                    else:
                        if cursor_only:
                            logger.info("No cursor after page %d with %d results — "
                                        "cursor_only mode, stopping",
                                        page, len(data) if isinstance(data, list) else 1)
                            break
                        # No cursor but we got results — switch to page numbers
                        # Continue from next page (no cooldown needed here)
                        logger.info("No cursor after page %d with %d results — "
                                    "switching to page-number pagination",
                                    page, len(data) if isinstance(data, list) else 1)
                        use_page_numbers = True

            time.sleep(self.delay)

        # Deduplicate
        total_raw = len(all_results)
        if dedup_key and all_results:
            all_results = self._deduplicate(all_results, dedup_key)

        metadata = {
            "page_count": page,
            "total_raw": total_raw,
            "total_deduped": len(all_results),
            "errors": errors,
            "limit_reached": limit_reached,
        }

        return all_results, metadata

    def call_action_async(self, action_slug, inputs_list, parameters=None):
        """
        Submit a batch async job. Returns run_uid or None on failure.
        Body uses "inputs" (plural) — the async schema.
        """
        url = f"{self.base_url}/actions/linkedin-{action_slug}/run/async"
        body = self._build_body(inputs_list, direct_mode=True, parameters=parameters, is_async=True)
        data, _, error = self._request_with_retry("POST", url, body=body)

        if error is not None:
            logger.error("Async submit failed: %s", error)
            return None

        if isinstance(data, dict):
            return data.get("run_uid") or data.get("uid") or data.get("id")
        return None

    def poll_async_run(self, run_uid, poll_interval=10, max_polls=360):
        """
        Poll /runs/{run_uid} until completion. Max polls = 360 at 10s = 1 hour.
        Returns (outputs, status) or (None, last_status) on failure.
        """
        status_url = f"{self.base_url}/runs/{run_uid}"
        outputs_url = f"{self.base_url}/runs/{run_uid}/outputs"

        for i in range(max_polls):
            time.sleep(poll_interval)
            data, _, error = self._request_with_retry("GET", status_url)

            if error is not None:
                logger.warning("Poll %d/%d error: %s", i + 1, max_polls, error)
                continue

            status = data.get("status", "UNKNOWN") if isinstance(data, dict) else "UNKNOWN"
            logger.info("Poll %d/%d: %s", i + 1, max_polls, status)

            if status in ("SUCCEEDED", "COMPLETED", "DONE"):
                out_data, _, out_error = self._request_with_retry("GET", outputs_url)
                if out_error is not None:
                    return None, status
                return out_data, status

            if status in ("FAILED", "ERROR", "CANCELLED"):
                logger.error("Async run %s ended with status: %s", run_uid, status)
                return None, status

        logger.error("Async run %s timed out after %d polls", run_uid, max_polls)
        return None, "TIMEOUT"

    # ── Internal Methods ──────────────────────────────────────────────────

    def _build_body(self, input_data, direct_mode, parameters, is_async=False):
        """Construct the request body with correct identity and input schema."""
        body = {}

        # Identity selection
        if direct_mode:
            body["identity_ids"] = [self.identity_uuid]
        else:
            body["identity_mode"] = "managed"

        # Input — singular for live, plural for async
        if is_async:
            body["inputs"] = input_data if input_data is not None else []
        else:
            body["input"] = input_data if input_data is not None else {}

        # Optional parameters (e.g., sort_order for commenters)
        if parameters:
            body["parameters"] = parameters

        return body

    def _request_with_retry(self, method, url, body=None, max_retries=3):
        """
        Single HTTP request with error classification and retry.
        Returns (data, headers, error_info).
        Max 3 retries — NEVER infinite loop.
        """
        last_error = None

        for attempt in range(max_retries + 1):
            try:
                if method == "GET":
                    resp = self.session.get(url, timeout=60)
                else:
                    resp = self.session.post(url, json=body, timeout=60)
            except requests.RequestException as e:
                last_error = {"error_label": "NETWORK_ERROR", "message": str(e)}
                if attempt < max_retries:
                    wait = 2 ** attempt
                    logger.warning("Network error (attempt %d/%d), retrying in %ds: %s",
                                   attempt + 1, max_retries, wait, e)
                    time.sleep(wait)
                    continue
                return None, {}, last_error

            # Parse response
            try:
                data = resp.json()
            except ValueError:
                last_error = {"error_label": "PARSE_ERROR", "message": resp.text[:500]}
                if attempt < max_retries:
                    wait = 2 ** attempt
                    logger.warning("Parse error (attempt %d/%d), retrying in %ds",
                                   attempt + 1, max_retries, wait)
                    time.sleep(wait)
                    continue
                return None, dict(resp.headers), last_error

            headers = dict(resp.headers)

            # Edges 429 (API-level rate limit) — read Retry-After header
            if resp.status_code == 429 and not isinstance(data, dict):
                retry_after = int(resp.headers.get("Retry-After", 2 ** attempt))
                last_error = {"error_label": "EDGES_429", "retry_after": retry_after}
                if attempt < max_retries:
                    logger.warning("Edges 429 (attempt %d/%d), waiting %ds",
                                   attempt + 1, max_retries, retry_after)
                    time.sleep(retry_after)
                    continue
                return None, headers, last_error

            # Check for error_label in response
            if isinstance(data, dict) and "error_label" in data:
                error_label = data["error_label"]
                error_info = {
                    "error_label": error_label,
                    "message": data.get("message", ""),
                    "error_ref": data.get("error_ref", ""),
                    "status_code": data.get("status_code", resp.status_code),
                }

                # Never-retry errors — return immediately
                if error_label in NO_RETRY_ERRORS:
                    logger.warning("Non-retryable error: %s — %s", error_label, error_info["message"])
                    return None, headers, error_info

                # Retryable errors — exponential backoff
                if error_label in RETRY_ERRORS and attempt < max_retries:
                    if error_label == "STATUS_429":
                        wait = int(data.get("retry_after", min(2 ** attempt * 2, 30)))
                    else:
                        wait = min(2 ** attempt * 2, 60)
                    logger.warning("%s (attempt %d/%d), retrying in %ds",
                                   error_label, attempt + 1, max_retries, wait)
                    time.sleep(wait)
                    continue

                # Retryable but exhausted, or unknown error
                return None, headers, error_info

            # Success — data can be list, dict, or even empty list []
            # Check `if data is None:` not `if not data:` because [] is valid
            return data, headers, None

        return None, {}, last_error

    @staticmethod
    def _deduplicate(results, key):
        """Deduplicate list of dicts by a field name. Keep first occurrence."""
        seen = set()
        unique = []
        for item in results:
            val = item.get(key)
            if val is None:
                # Keep items without the dedup key
                unique.append(item)
            elif val not in seen:
                seen.add(val)
                unique.append(item)
        return unique
