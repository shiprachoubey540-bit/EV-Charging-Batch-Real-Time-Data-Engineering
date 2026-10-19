"""
ingestion/download.py — Robust, checkpointed downloader for ACN-Data historical sessions.

Owner: Ananthan S

This module implements a resilient, paginated client for the official
Caltech ACN-Data REST API (https://ev.caltech.edu/api/v1/).

Key guarantees:
- Follows HATEOAS ``_links.next`` URLs until no next link exists.
- Strictly validates API host before making any HTTP request and blocks unsafe redirects.
- Reads ``ACNDATA_API_TOKEN`` from environment; never exposes secrets in logs or errors.
- Automatically normalizes query boundaries to RFC 2822 GMT format.
- Bounded retries with exponential backoff and ``Retry-After`` header support.
- Stores pages atomically in ``data/raw/chunks/``.
- Resumes interrupted downloads using validated, sequential checkpoints.
- Detects circular pagination loops and mid-stream shifts in API reported totals.
- Assembles candidate dataset into ``data/raw/acndata_sessions_full_candidate.json``.
- Never modifies or overwrites the original ``data/raw/acndata_sessions.json``.
- Produces evidence-based completeness evaluation in ``data/raw/download_report.json``.
- Fully compatible with ``ingestion/extract.py``.
"""

import argparse
import base64
import json
import logging
import os
import random
import re
import socket
import sys
import time
from datetime import datetime, timezone
from email.utils import format_datetime, parsedate_to_datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urljoin, urlparse, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from dotenv import dotenv_values, load_dotenv

# Ensure project root .env is loaded (override=True ensures file updates take precedence)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env", override=True)

logger = logging.getLogger(__name__)

EXPECTED_API_HOST = "ev.caltech.edu"
DEFAULT_BASE_URL = "https://ev.caltech.edu/api/v1"
DEFAULT_RAW_DIR = os.getenv("DATA_RAW_DIR", "./data/raw")
ORIGINAL_DATASET_FILENAME = "acndata_sessions.json"
CANDIDATE_FILENAME = "acndata_sessions_full_candidate.json"
CHECKPOINT_FILENAME = ".acndata_download_checkpoint.json"
REPORT_FILENAME = "download_report.json"
CHUNKS_DIRNAME = "chunks"

TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}


class ACNDownloadError(Exception):
    """Raised when an ACN-Data download operation encounters an unrecoverable error."""
    pass


class SafeRedirectHandler(HTTPRedirectHandler):
    """
    HTTP redirect handler that ensures redirects remain strictly on HTTPS
    and on the expected ACN-Data API host. Strips Authorization headers
    if redirect target changes origin or scheme.
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        base_url = getattr(req, "full_url", "")
        if isinstance(base_url, str) and base_url:
            resolved_url = urljoin(base_url, newurl)
        else:
            resolved_url = newurl

        try:
            validate_api_url(resolved_url, EXPECTED_API_HOST)
        except ValueError as exc:
            logger.error("Blocked unsafe redirect to '%s': %s", resolved_url, exc)
            return None  # Disallow redirect; causes HTTPError to be raised to caller

        new_req = super().redirect_request(req, fp, code, msg, headers, resolved_url)
        if new_req is not None and isinstance(base_url, str) and base_url:
            orig_parsed = urlparse(base_url)
            new_parsed = urlparse(resolved_url)
            if (orig_parsed.hostname != new_parsed.hostname) or (orig_parsed.scheme != new_parsed.scheme):
                if hasattr(new_req, "headers"):
                    new_req.headers.pop("Authorization", None)
                    new_req.headers.pop("authorization", None)
                if hasattr(new_req, "unredirected_hdrs"):
                    new_req.unredirected_hdrs.pop("Authorization", None)
                    new_req.unredirected_hdrs.pop("authorization", None)
        return new_req


def mask_token(token: str | None) -> str:
    """Mask sensitive tokens for safe logging and error reporting."""
    if not token:
        return "[NOT_SET]"
    if len(token) <= 8:
        return "[REDACTED]"
    return f"{token[:3]}...{token[-3:]}"


def sanitize_message(msg: str, token: str | None) -> str:
    """Remove any raw token string or base64 authorization from messages."""
    if not msg:
        return ""
    clean = msg
    # Scrub common Authorization header patterns first
    clean = re.sub(r"Basic\s+[A-Za-z0-9+/=]+", "Basic [REDACTED]", clean)
    if token:
        clean = clean.replace(token, mask_token(token))
        b64_token = base64.b64encode(f"{token}:".encode()).decode()
        clean = clean.replace(b64_token, "[REDACTED]")
    return clean


def sanitize_url(url: str) -> str:
    """
    Ensure a URL contains only valid HTTP characters by percent-encoding
    unescaped spaces, quotes, and control characters in path and query,
    while preserving valid URL syntax and existing percent-encodings.
    """
    if not url:
        return ""
    parts = urlsplit(url)
    clean_path = quote(parts.path, safe="/%:@")
    clean_query = quote(parts.query, safe="=&+:%~[]")
    return urlunsplit((parts.scheme, parts.netloc, clean_path, clean_query, parts.fragment))


def validate_api_url(url: str, allowed_host: str = EXPECTED_API_HOST) -> str:
    """
    Validate that a URL targets the expected API host and uses HTTPS.

    Parameters
    ----------
    url : str
        Target URL to validate.
    allowed_host : str
        The only allowed hostname (default: ev.caltech.edu).

    Returns
    -------
    str
        The validated URL.

    Raises
    ------
    ValueError
        If the scheme is not https or hostname does not match allowed_host.
    """
    parsed = urlparse(url)
    if parsed.scheme.lower() != "https":
        raise ValueError(
            f"Disallowed URL scheme '{parsed.scheme}': only https is permitted"
        )
    if parsed.hostname and parsed.hostname.lower() != allowed_host.lower():
        raise ValueError(
            f"Disallowed target host '{parsed.hostname}': only {allowed_host} is permitted"
        )
    return url


def diagnose_auth_config(env_path: Path | str | None = None) -> dict:
    """
    Safely diagnose authentication configuration without exposing secrets, lengths, or hashes.

    Distinguishes missing token, placeholder token, environment variable mismatch,
    and validly encoded credentials awaiting server validation.
    """
    target_env = Path(env_path).resolve() if env_path else (PROJECT_ROOT / ".env")
    env_file_exists = target_env.is_file()

    file_vars = dotenv_values(target_env) if env_file_exists else {}
    raw_file_val = file_vars.get("ACNDATA_API_TOKEN")

    process_val = os.environ.get("ACNDATA_API_TOKEN")
    env_mismatch = bool(
        raw_file_val is not None and process_val is not None and raw_file_val != process_val
    )

    effective_token = raw_file_val if raw_file_val is not None else process_val
    token_present = effective_token is not None and len(effective_token.strip()) > 0

    known_placeholders = {
        "your_acndata_api_token",
        "your_actual_token_here",
        "placeholder",
        "your_token_here",
        "your_api_token",
    }
    is_placeholder = bool(effective_token and effective_token.strip().lower() in known_placeholders)

    has_formatting_issue = False
    if effective_token:
        has_formatting_issue = bool(
            effective_token != effective_token.strip()
            or effective_token.startswith(('"', "'"))
            or effective_token.endswith(('"', "'"))
        )

    basic_auth_valid = False
    if token_present and not is_placeholder:
        clean_tok = effective_token.strip().strip('"').strip("'")
        try:
            token_bytes = f"{clean_tok}:".encode("utf-8")
            b64_str = base64.b64encode(token_bytes).decode("ascii")
            decoded = base64.b64decode(b64_str).decode("utf-8")
            basic_auth_valid = decoded.endswith(":") and decoded[:-1] == clean_tok
        except Exception:
            basic_auth_valid = False

    if not token_present:
        status = "MISSING_TOKEN"
    elif is_placeholder:
        status = "PLACEHOLDER_TOKEN"
    elif env_mismatch:
        status = "ENV_MISMATCH"
    elif has_formatting_issue:
        status = "FORMATTING_ISSUE"
    elif basic_auth_valid:
        status = "CONFIGURED_AWAITING_SERVER_VALIDATION"
    else:
        status = "ENCODING_FAILURE"

    return {
        "status": status,
        "env_file_exists": env_file_exists,
        "token_configured": token_present,
        "is_placeholder": is_placeholder,
        "has_formatting_issue": has_formatting_issue,
        "env_mismatch_detected": env_mismatch,
        "basic_auth_encoding_valid": basic_auth_valid,
    }


def normalize_query_timestamp(ts: str | datetime | None) -> str | None:
    """
    Normalize timestamps (ISO or RFC 2822) to standard RFC 2822 GMT format.

    ACN-Data stores connectionTime in RFC 2822 GMT strings (e.g., 'Tue, 01 Jan 2019 16:18:00 GMT').
    Normalizing ensures MongoDB query comparisons match the database representation.
    """
    if ts is None:
        return None
    if isinstance(ts, datetime):
        dt = ts.astimezone(timezone.utc) if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
        return format_datetime(dt, usegmt=True)

    ts_str = str(ts).strip()
    # Try RFC 2822 first
    try:
        dt = parsedate_to_datetime(ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return format_datetime(dt.astimezone(timezone.utc), usegmt=True)
    except Exception:
        pass

    # Try ISO 8601
    try:
        iso_str = ts_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return format_datetime(dt.astimezone(timezone.utc), usegmt=True)
    except Exception:
        pass

    # Return raw if unparseable
    return ts_str


def _atomic_write_json(target_path: Path, data: dict | list) -> None:
    """Write data to target_path atomically using a temporary file in the same directory."""
    if target_path.name == ORIGINAL_DATASET_FILENAME:
        raise ACNDownloadError(
            f"Safety violation: refused to write to protected original dataset file '{ORIGINAL_DATASET_FILENAME}'"
        )
    target_path.parent.mkdir(parents=True, exist_ok=True)
    temp_file = target_path.with_name(f"{target_path.name}.tmp.{os.getpid()}")
    try:
        temp_file.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
        temp_file.replace(target_path)
    finally:
        if temp_file.exists():
            try:
                temp_file.unlink()
            except OSError:
                pass


class ACNDataDownloader:
    """
    Resilient, paginated HTTP downloader for the ACN-Data REST API.

    Attributes
    ----------
    api_token : str | None
        User API token from ACN Portal.
    base_url : str
        Base API URL.
    raw_dir : Path
        Directory for raw downloads and chunks.
    timeout_sec : float
        HTTP timeout per request in seconds.
    max_retries : int
        Maximum retries for transient HTTP / network errors.
    backoff_factor : float
        Multiplier for exponential backoff.
    max_backoff_sec : float
        Cap on backoff sleep in seconds.
    """

    def __init__(
        self,
        api_token: str | None = None,
        base_url: str = DEFAULT_BASE_URL,
        raw_dir: str | Path = DEFAULT_RAW_DIR,
        timeout_sec: float = 30.0,
        max_retries: int = 5,
        backoff_factor: float = 1.0,
        max_backoff_sec: float = 30.0,
    ) -> None:
        load_dotenv(PROJECT_ROOT / ".env", override=True)
        self.api_token = api_token or os.getenv("ACNDATA_API_TOKEN")
        self.base_url = base_url.rstrip("/")
        self.raw_dir = Path(raw_dir).resolve()
        self.timeout_sec = float(timeout_sec)
        self.max_retries = int(max_retries)
        self.backoff_factor = float(backoff_factor)
        self.max_backoff_sec = float(max_backoff_sec)

        self.chunks_dir = self.raw_dir / CHUNKS_DIRNAME
        self.checkpoint_file = self.raw_dir / CHECKPOINT_FILENAME
        self.candidate_file = self.raw_dir / CANDIDATE_FILENAME
        self.report_file = self.raw_dir / REPORT_FILENAME

        # Safety check: candidate file must never point to original dataset file
        if (
            self.candidate_file.name == ORIGINAL_DATASET_FILENAME
            or self.candidate_file.resolve() == (self.raw_dir / ORIGINAL_DATASET_FILENAME).resolve()
        ):
            raise ValueError(
                f"Refusing to configure candidate_file pointing to protected original dataset '{ORIGINAL_DATASET_FILENAME}'"
            )

        # Build secure opener with SafeRedirectHandler
        self.opener = build_opener(SafeRedirectHandler())

    def _build_auth_header(self) -> str | None:
        """Construct Basic Auth header from API token."""
        if not self.api_token:
            return None
        token_bytes = f"{self.api_token}:".encode("utf-8")
        b64_auth = base64.b64encode(token_bytes).decode("ascii")
        return f"Basic {b64_auth}"

    def build_initial_url(
        self,
        site: str = "caltech",
        start_time: str | None = None,
        end_time: str | None = None,
        min_kwh: float | None = None,
        extra_where: str | None = None,
        sort: str = "-connectionTime",
    ) -> str:
        """
        Build the starting endpoint URL with normalized RFC 2822 GMT dates.
        Default sort is '-connectionTime' for ascending chronological order
        (ACN-Data default sort is descending without minus prefix).
        """
        norm_start = normalize_query_timestamp(start_time)
        norm_end = normalize_query_timestamp(end_time)

        where_parts = []
        if norm_start:
            where_parts.append(f'connectionTime>="{norm_start}"')
        if norm_end:
            where_parts.append(f'connectionTime<="{norm_end}"')
        if min_kwh is not None:
            where_parts.append(f"kWhDelivered>={min_kwh}")
        if extra_where:
            where_parts.append(extra_where)

        query_params = {}
        if where_parts:
            query_params["where"] = " and ".join(where_parts)
        if sort:
            query_params["sort"] = sort

        encoded = urlencode(query_params)
        endpoint = f"{self.base_url}/sessions/{site}"
        return f"{endpoint}?{encoded}" if encoded else endpoint

    def fetch_page(self, url: str) -> dict:
        """
        Fetch a single page with retries, exponential backoff, and host validation.
        """
        url = sanitize_url(url)
        validate_api_url(url)
        headers = {"Accept": "application/json", "User-Agent": "EV-Charging-ETL/1.0"}
        auth = self._build_auth_header()
        if auth:
            headers["Authorization"] = auth

        attempt = 0
        last_error: Exception | None = None

        while attempt <= self.max_retries:
            attempt += 1
            req = Request(url, headers=headers, method="GET")

            try:
                logger.debug("HTTP GET: %s (attempt %d/%d)", url, attempt, self.max_retries + 1)
                with self.opener.open(req, timeout=self.timeout_sec) as resp:
                    raw_body = resp.read().decode("utf-8")
                    data = json.loads(raw_body)
                    if not isinstance(data, dict) or "_items" not in data:
                        raise ValueError(
                            "Malformed API response: root must be dict containing '_items'"
                        )
                    return data

            except HTTPError as exc:
                last_error = exc
                status = exc.code
                if status not in TRANSIENT_STATUS_CODES or attempt > self.max_retries:
                    err_msg = sanitize_message(
                        f"HTTP {status} {exc.reason} while requesting {url}", self.api_token
                    )
                    logger.error("Non-transient or exhausted HTTP error: %s", err_msg)
                    raise ACNDownloadError(err_msg) from None

                # Calculate backoff with Retry-After support
                retry_after_hdr = exc.headers.get("Retry-After")
                sleep_sec = None
                if retry_after_hdr and retry_after_hdr.isdigit():
                    sleep_sec = min(float(retry_after_hdr), self.max_backoff_sec)

                if sleep_sec is None:
                    backoff = self.backoff_factor * (2 ** (attempt - 1))
                    jitter = random.uniform(0.1, 0.5)
                    sleep_sec = min(backoff + jitter, self.max_backoff_sec)

                logger.warning(
                    "HTTP %d (transient) on attempt %d/%d. Retrying in %.2fs...",
                    status,
                    attempt,
                    self.max_retries + 1,
                    sleep_sec,
                )
                time.sleep(sleep_sec)

            except (URLError, TimeoutError, socket.timeout, ConnectionResetError) as exc:
                last_error = exc
                if attempt > self.max_retries:
                    err_msg = sanitize_message(
                        f"Network error exhausted max retries: {exc}", self.api_token
                    )
                    logger.error("%s", err_msg)
                    raise ACNDownloadError(err_msg) from None

                backoff = self.backoff_factor * (2 ** (attempt - 1))
                sleep_sec = min(backoff + random.uniform(0.1, 0.5), self.max_backoff_sec)
                logger.warning(
                    "Network error (%s) on attempt %d/%d. Retrying in %.2fs...",
                    type(exc).__name__,
                    attempt,
                    self.max_retries + 1,
                    sleep_sec,
                )
                time.sleep(sleep_sec)

            except json.JSONDecodeError as exc:
                err_msg = f"Strict JSON parsing failed on API response: {exc.msg} at line {exc.lineno}"
                logger.error("%s", err_msg)
                raise ACNDownloadError(err_msg) from None

        raise ACNDownloadError(f"Failed to fetch page after {self.max_retries} retries: {last_error}")

    def _load_and_validate_checkpoint(self, site: str, query_filter: str) -> dict | None:
        """
        Validate existing checkpoint and chunk files. Returns None if invalid, stale, or discontinuous.
        """
        if not self.checkpoint_file.exists():
            return None

        try:
            ckpt = json.loads(self.checkpoint_file.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            logger.warning("Checkpoint file is corrupted; ignoring and restarting.")
            return None

        if ckpt.get("site") != site or ckpt.get("query_filter") != query_filter:
            logger.warning("Checkpoint matches a different site or query; ignoring and restarting.")
            return None

        chunk_files = ckpt.get("chunk_files", [])
        if not chunk_files:
            return None

        # Verify sequential page numbering in chunk filenames
        expected_indices = list(range(1, len(chunk_files) + 1))
        actual_indices = []
        for cf in chunk_files:
            p = Path(cf)
            if not p.exists() or p.stat().st_size == 0:
                logger.warning("Chunk file %s missing or empty; invalidating checkpoint.", p)
                return None
            m = re.search(r"page_(\d+)", p.name)
            if not m:
                logger.warning("Chunk file %s has invalid filename; invalidating checkpoint.", p)
                return None
            actual_indices.append(int(m.group(1)))
            try:
                cdata = json.loads(p.read_text(encoding="utf-8"))
                if not isinstance(cdata, dict) or "_items" not in cdata:
                    return None
            except json.JSONDecodeError:
                logger.warning("Chunk file %s is malformed JSON; invalidating checkpoint.", p)
                return None

        if actual_indices != expected_indices:
            logger.warning(
                "Chunk sequence is not strictly continuous (%s vs %s); invalidating checkpoint.",
                actual_indices,
                expected_indices,
            )
            return None

        return ckpt

    def download_dataset(
        self,
        site: str = "caltech",
        start_time: str = "Tue, 01 Jan 2019 16:18:00 GMT",
        end_time: str = "Tue, 31 Dec 2024 16:19:00 GMT",
        min_kwh: float | None = None,
        max_pages: int | None = None,
    ) -> dict:
        """
        Download historical charging sessions using official API pagination.
        """
        # Safety check: candidate file must never point to original dataset file
        if self.candidate_file.resolve() == (self.raw_dir / ORIGINAL_DATASET_FILENAME).resolve():
            raise ValueError(
                f"Refusing to write to protected original dataset path: {self.candidate_file}"
            )

        norm_start = normalize_query_timestamp(start_time)
        norm_end = normalize_query_timestamp(end_time)

        download_start_ts = datetime.now(timezone.utc).isoformat()
        self.chunks_dir.mkdir(parents=True, exist_ok=True)

        initial_url = self.build_initial_url(
            site=site, start_time=norm_start, end_time=norm_end, min_kwh=min_kwh
        )
        query_filter = initial_url.split("?", 1)[1] if "?" in initial_url else ""

        # Check for valid checkpoint
        ckpt = self._load_and_validate_checkpoint(site, query_filter)
        if ckpt:
            logger.info(
                "Resuming download from checkpoint: page %d, %d records already cached",
                ckpt["last_page_index"] + 1,
                ckpt["total_records_so_far"],
            )
            next_url = sanitize_url(ckpt["next_url"]) if ckpt.get("next_url") else None
            page_index = ckpt["last_page_index"] + 1
            chunk_files = list(ckpt["chunk_files"])
            initial_reported_total = ckpt.get("initial_reported_total")
            last_reported_total = ckpt.get("last_reported_total")
        else:
            logger.info("Starting fresh download for site '%s' (filter: %s)", site, query_filter)
            next_url = initial_url
            page_index = 1
            chunk_files = []
            initial_reported_total = None
            last_reported_total = None

        failures: list[dict] = []
        inconsistent_totals_detected: list[dict] = []
        seen_urls: set[str] = set()
        seen_pages: set[int] = set()

        # If resuming from checkpoint, record already seen chunk pages in seen_pages
        if ckpt:
            for cf in chunk_files:
                m = re.search(r"page_(\d+)", Path(cf).name)
                if m:
                    seen_pages.add(int(m.group(1)))

        # Pagination loop following _links.next
        while next_url:
            if max_pages is not None and page_index > max_pages:
                logger.info("Reached maximum requested pages limit (%d)", max_pages)
                break

            # Cycle and host validation
            if next_url in seen_urls:
                err_msg = f"Circular pagination detected: URL already visited ({next_url})"
                logger.error("%s", err_msg)
                failures.append({"page_index": page_index, "url": next_url, "error": err_msg})
                break
            seen_urls.add(next_url)

            try:
                validate_api_url(next_url, EXPECTED_API_HOST)
            except ValueError as exc:
                err_msg = f"Pagination link escaped expected API host: {exc}"
                logger.error("%s", err_msg)
                failures.append({"page_index": page_index, "url": next_url, "error": err_msg})
                break

            logger.info("Downloading page %d: %s", page_index, next_url)
            try:
                page_data = self.fetch_page(next_url)
            except Exception as exc:
                failures.append({"page_index": page_index, "url": next_url, "error": str(exc)})
                logger.error("Download halted at page %d due to error: %s", page_index, exc)
                break

            # Metadata and total consistency check
            meta = page_data.get("_meta", {})
            current_page_num = meta.get("page")
            if current_page_num is not None and current_page_num in seen_pages:
                err_msg = f"Circular pagination detected: page {current_page_num} was already visited"
                logger.error("%s", err_msg)
                failures.append({"page_index": page_index, "url": next_url, "error": err_msg})
                break
            if current_page_num is not None:
                seen_pages.add(current_page_num)

            current_total = meta.get("total")
            if initial_reported_total is None and current_total is not None:
                initial_reported_total = current_total
                last_reported_total = current_total
            elif current_total is not None and current_total != last_reported_total:
                err_msg = (
                    f"Result set changed mid-download at page {page_index}: "
                    f"total shifted from {last_reported_total} to {current_total}"
                )
                logger.error("%s", err_msg)
                inconsistent_totals_detected.append(
                    {"page": page_index, "previous_total": last_reported_total, "new_total": current_total}
                )
                failures.append({"page_index": page_index, "url": next_url, "error": err_msg})
                last_reported_total = current_total
                # Halt immediately to avoid combining incompatible pages from different versions
                break

            # Save page chunk atomically
            chunk_file = self.chunks_dir / f"page_{page_index:05d}.json"
            _atomic_write_json(chunk_file, page_data)
            chunk_files.append(str(chunk_file))

            # Determine next link
            links = page_data.get("_links", {})
            next_link_info = links.get("next")
            if next_link_info and isinstance(next_link_info, dict) and "href" in next_link_info:
                next_href = next_link_info["href"]
                next_url = sanitize_url(urljoin(self.base_url + "/", next_href))
            else:
                next_url = None

            # Update checkpoint atomically
            ckpt_state = {
                "site": site,
                "query_filter": query_filter,
                "last_page_index": page_index,
                "next_url": next_url,
                "initial_reported_total": initial_reported_total,
                "last_reported_total": last_reported_total,
                "total_records_so_far": sum(
                    len(json.loads(Path(f).read_text(encoding="utf-8")).get("_items", []))
                    for f in chunk_files
                ),
                "chunk_files": chunk_files,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            _atomic_write_json(self.checkpoint_file, ckpt_state)
            page_index += 1

        download_end_ts = datetime.now(timezone.utc).isoformat()

        # Validate all chunk files and assemble candidate dataset
        expected_indices = list(range(1, len(chunk_files) + 1))
        actual_indices = []
        chunk_validation_errors: list[str] = []

        all_sessions = []
        seen_ids = set()
        duplicate_records_count = 0
        malformed_records_count = 0
        missing_required_fields: dict[str, int] = {
            "sessionID": 0,
            "siteID": 0,
            "clusterID": 0,
            "stationID": 0,
            "spaceID": 0,
            "connectionTime": 0,
        }

        for cf in chunk_files:
            cfp = Path(cf)
            if not cfp.exists() or cfp.stat().st_size == 0:
                chunk_validation_errors.append(f"Chunk file {cfp.name} is missing or empty")
                continue
            m = re.search(r"page_(\d+)", cfp.name)
            if not m:
                chunk_validation_errors.append(f"Chunk file {cfp.name} has invalid naming format")
                continue
            actual_indices.append(int(m.group(1)))
            try:
                cdata = json.loads(cfp.read_text(encoding="utf-8"))
                if not isinstance(cdata, dict) or "_items" not in cdata:
                    chunk_validation_errors.append(f"Chunk file {cfp.name} lacks valid '_items' structure")
                    continue
                items = cdata.get("_items", [])
                for item in items:
                    if not isinstance(item, dict):
                        malformed_records_count += 1
                        continue
                    rec_id = item.get("_id")
                    if not rec_id:
                        malformed_records_count += 1
                    elif rec_id in seen_ids:
                        duplicate_records_count += 1
                    else:
                        seen_ids.add(rec_id)

                    for req_fld in missing_required_fields:
                        if not item.get(req_fld):
                            missing_required_fields[req_fld] += 1

                    all_sessions.append(item)
            except json.JSONDecodeError as exc:
                chunk_validation_errors.append(f"Chunk file {cfp.name} is malformed JSON: {exc}")

        if not chunk_validation_errors and actual_indices != expected_indices:
            chunk_validation_errors.append(
                f"Discontinuous chunk sequence before assembly: {actual_indices} vs {expected_indices}"
            )

        # Completeness evaluation
        actual_min_dt = None
        actual_max_dt = None
        for s in all_sessions:
            conn = s.get("connectionTime")
            if conn:
                try:
                    dt = parsedate_to_datetime(conn).astimezone(timezone.utc)
                    if actual_min_dt is None or dt < actual_min_dt:
                        actual_min_dt = dt
                    if actual_max_dt is None or dt > actual_max_dt:
                        actual_max_dt = dt
                except Exception:
                    continue

        date_coverage_complete = False
        try:
            req_end_dt = parsedate_to_datetime(norm_end).astimezone(timezone.utc)
            if actual_max_dt:
                diff_days = (req_end_dt - actual_max_dt).total_seconds() / 86400.0
                date_coverage_complete = diff_days <= 1.0
        except Exception:
            pass

        status_reasons = []
        if failures:
            status_reasons.append(f"Encountered {len(failures)} unhandled request failure(s)")
        if chunk_validation_errors:
            status_reasons.extend(chunk_validation_errors)
        if next_url is not None:
            status_reasons.append("Download stopped before next link chain was exhausted")
        if last_reported_total is not None and len(all_sessions) != last_reported_total:
            status_reasons.append(
                f"Record count mismatch: assembled {len(all_sessions)} vs reported total {last_reported_total}"
            )
        if inconsistent_totals_detected:
            status_reasons.append(
                f"Inconsistent totals detected during pagination ({len(inconsistent_totals_detected)} shifts)"
            )
        if not date_coverage_complete:
            status_reasons.append("Date range does not span the requested end timestamp")

        completeness_verified = len(status_reasons) == 0 and len(all_sessions) > 0
        final_status = "COMPLETE" if completeness_verified else "INCOMPLETE"

        # Assemble candidate dataset with explicit completeness status
        candidate_data = {
            "_meta": {
                "site": site,
                "start": norm_start,
                "end": norm_end,
                "total_extracted": len(all_sessions),
                "api_reported_total": last_reported_total,
                "pages_assembled": len(chunk_files),
                "assembled_at": download_end_ts,
                "is_complete": completeness_verified,
                "download_status": final_status,
                "status_reasons": status_reasons,
            },
            "_items": all_sessions,
        }

        # Write candidate file atomically
        _atomic_write_json(self.candidate_file, candidate_data)
        candidate_bytes = self.candidate_file.stat().st_size

        report = {
            "download_status": final_status,
            "completeness_verified": completeness_verified,
            "status_reasons": status_reasons,
            "site": site,
            "requested_filters": {
                "start": norm_start,
                "end": norm_end,
                "min_kwh": min_kwh,
            },
            "retrieval_started_at": download_start_ts,
            "retrieval_completed_at": download_end_ts,
            "pages_downloaded": len(chunk_files),
            "api_reported_total": last_reported_total,
            "actual_records_downloaded": len(all_sessions),
            "unique_records_count": len(seen_ids),
            "duplicate_records_count": duplicate_records_count,
            "malformed_records_count": malformed_records_count,
            "missing_required_fields": missing_required_fields,
            "checkpoint_resumed": bool(ckpt is not None),
            "earliest_connection_time": actual_min_dt.isoformat() if actual_min_dt else None,
            "latest_connection_time": actual_max_dt.isoformat() if actual_max_dt else None,
            "date_coverage_complete": date_coverage_complete,
            "unverified_assumptions": [
                "Assumes Caltech ACN-Data REST API sorts ascending chronologically when sort=-connectionTime is specified.",
                "Assumes API-reported '_meta.total' accurately reflects total matching records in the collection.",
                "Assumes no concurrent upstream deletions or additions in the requested historical window during download.",
                "Assumes records missing 'connectionTime' are outside the intended time window.",
            ],
            "candidate_file": str(self.candidate_file),
            "candidate_file_bytes": candidate_bytes,
            "failures": failures,
            "inconsistent_totals": inconsistent_totals_detected,
        }

        _atomic_write_json(self.report_file, report)
        logger.info(
            "Download finished: status=%s, records=%d, pages=%d, completeness_verified=%s",
            final_status,
            len(all_sessions),
            len(chunk_files),
            completeness_verified,
        )

        return report


def main() -> None:
    """CLI entry point for downloading ACN-Data historical sessions."""
    parser = argparse.ArgumentParser(
        description="Download historical charging sessions from the ACN-Data REST API."
    )
    parser.add_argument(
        "--site",
        type=str,
        default="caltech",
        help="ACN site identifier (default: caltech)",
    )
    parser.add_argument(
        "--start",
        type=str,
        default="Tue, 01 Jan 2019 16:18:00 GMT",
        help="Start timestamp (RFC 2822 or ISO 8601, default: Tue, 01 Jan 2019 16:18:00 GMT)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default="Tue, 31 Dec 2024 16:19:00 GMT",
        help="End timestamp (RFC 2822 or ISO 8601, default: Tue, 31 Dec 2024 16:19:00 GMT)",
    )
    parser.add_argument(
        "--min-kwh",
        type=float,
        default=None,
        help="Minimum kWh delivered filter threshold",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Max pages to download (useful for bounded testing)",
    )
    parser.add_argument(
        "--raw-dir",
        type=str,
        default=DEFAULT_RAW_DIR,
        help="Directory to save raw downloads (default: ./data/raw)",
    )
    parser.add_argument(
        "--diagnose-auth",
        action="store_true",
        help="Run safe offline authentication configuration diagnostic and exit",
    )
    args = parser.parse_args()

    if args.diagnose_auth:
        diag = diagnose_auth_config()
        print(json.dumps(diag, indent=2))
        return

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )

    downloader = ACNDataDownloader(raw_dir=args.raw_dir)
    report = downloader.download_dataset(
        site=args.site,
        start_time=args.start,
        end_time=args.end,
        min_kwh=args.min_kwh,
        max_pages=args.max_pages,
    )
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
