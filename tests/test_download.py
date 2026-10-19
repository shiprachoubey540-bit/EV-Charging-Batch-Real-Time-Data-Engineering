"""Tests for ingestion.download — Mocked ACN-Data Downloader.

All tests run completely offline without network calls or live credentials.
"""

import io
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError, URLError

import pytest

from ingestion.download import (
    ACNDataDownloader,
    ACNDownloadError,
    SafeRedirectHandler,
    mask_token,
    normalize_query_timestamp,
    sanitize_message,
    sanitize_url,
    validate_api_url,
)
from ingestion.extract import extract_sessions


# ======================================================================
# URL VALIDATION & SECURITY TESTS
# ======================================================================


def test_validate_api_url_valid():
    """Valid HTTPS URL to ev.caltech.edu must pass validation."""
    url = "https://ev.caltech.edu/api/v1/sessions/caltech?page=1"
    assert validate_api_url(url) == url


def test_validate_api_url_rejects_insecure_http():
    """Plain HTTP must be rejected to prevent cleartext credential leakage."""
    with pytest.raises(ValueError, match="only https is permitted"):
        validate_api_url("http://ev.caltech.edu/api/v1/sessions/caltech")


def test_validate_api_url_rejects_foreign_host():
    """URLs pointing outside ev.caltech.edu must be rejected."""
    with pytest.raises(ValueError, match="only ev.caltech.edu is permitted"):
        validate_api_url("https://malicious-site.com/api/v1/sessions")


def test_safe_redirect_handler_blocks_insecure_redirect():
    """Redirects to plain HTTP or external hosts must return None and be blocked."""
    handler = SafeRedirectHandler()
    req = MagicMock()
    fp = MagicMock()
    headers = MagicMock()

    # Insecure HTTP redirect
    res_insecure = handler.redirect_request(req, fp, 302, "Found", headers, "http://ev.caltech.edu/api/v1/sessions")
    assert res_insecure is None

    # Foreign host redirect
    res_foreign = handler.redirect_request(req, fp, 302, "Found", headers, "https://phishing.com/api/v1/sessions")
    assert res_foreign is None


def test_mask_token():
    """Verify secret token masking behavior."""
    assert mask_token(None) == "[NOT_SET]"
    assert mask_token("") == "[NOT_SET]"
    assert mask_token("short") == "[REDACTED]"
    assert mask_token("secret_token_12345") == "sec...345"


def test_sanitize_message_scrubs_token_and_auth():
    """Raw token and Authorization headers must be scrubbed from messages."""
    token = "super_secret_acn_token_xyz"
    msg = f"Failed requesting with token {token} and header Basic c3VwZXJfc2VjcmV0X2Fjbl90b2tlbjp="
    sanitized = sanitize_message(msg, token)
    assert token not in sanitized
    assert "sup...xyz" in sanitized
    assert "c3VwZXJfc2VjcmV0" not in sanitized


def test_normalize_query_timestamp():
    """ISO 8601 and datetime objects must normalize to RFC 2822 GMT format."""
    # From ISO string
    norm_iso = normalize_query_timestamp("2019-01-01T00:00:00Z")
    assert "Tue, 01 Jan 2019" in norm_iso
    assert "GMT" in norm_iso

    # From datetime object
    dt = datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    norm_dt = normalize_query_timestamp(dt)
    assert "Tue, 31 Dec 2024 23:59:59 GMT" == norm_dt

    # Already RFC 2822 string
    rfc_raw = "Tue, 01 Jan 2019 16:18:00 GMT"
    assert normalize_query_timestamp(rfc_raw) == rfc_raw


# ======================================================================
# MOCKED DOWNLOADER TESTS
# ======================================================================


@pytest.fixture()
def downloader(tmp_path):
    """Fixture returning an ACNDataDownloader configured with a temp directory."""
    return ACNDataDownloader(
        api_token="test_mock_token_12345",
        base_url="https://ev.caltech.edu/api/v1",
        raw_dir=tmp_path,
        timeout_sec=5.0,
        max_retries=2,
        backoff_factor=0.01,  # Fast backoff for tests
        max_backoff_sec=0.05,
    )


def _make_mock_response(data: dict):
    """Create a mock context-manager response object for opener.open."""
    raw_bytes = json.dumps(data).encode("utf-8")
    mock_resp = MagicMock()
    mock_resp.read.return_value = raw_bytes
    mock_resp.__enter__.return_value = mock_resp
    return mock_resp


def test_download_single_page_success(downloader, tmp_path):
    """Single page with no next link should complete and assemble candidate JSON."""
    page_1 = {
        "_items": [
            {"_id": "rec_001", "sessionID": "s_001", "connectionTime": "Tue, 01 Jan 2019 17:00:00 GMT"},
            {"_id": "rec_002", "sessionID": "s_002", "connectionTime": "Tue, 31 Dec 2024 16:00:00 GMT"},
        ],
        "_meta": {"page": 1, "max_results": 25, "total": 2},
        "_links": {"self": {"href": "sessions/caltech?page=1"}},
    }

    with patch.object(downloader.opener, "open", return_value=_make_mock_response(page_1)):
        report = downloader.download_dataset(
            site="caltech",
            start_time="Tue, 01 Jan 2019 16:18:00 GMT",
            end_time="Tue, 31 Dec 2024 16:19:00 GMT",
        )

    assert report["download_status"] == "COMPLETE"
    assert report["completeness_verified"] is True
    assert report["actual_records_downloaded"] == 2
    assert report["pages_downloaded"] == 1

    candidate_file = tmp_path / "acndata_sessions_full_candidate.json"
    assert candidate_file.exists()
    candidate_json = json.loads(candidate_file.read_text(encoding="utf-8"))
    assert len(candidate_json["_items"]) == 2
    assert candidate_json["_meta"]["total_extracted"] == 2
    assert candidate_json["_meta"]["is_complete"] is True


def test_download_pagination_traversal(downloader, tmp_path):
    """Downloader should follow _links.next until exhausted."""
    page_1 = {
        "_items": [{"_id": "rec_001", "connectionTime": "Tue, 01 Jan 2019 17:00:00 GMT"}],
        "_meta": {"page": 1, "max_results": 1, "total": 2},
        "_links": {"next": {"href": "sessions/caltech?page=2"}},
    }
    page_2 = {
        "_items": [{"_id": "rec_002", "connectionTime": "Tue, 31 Dec 2024 16:00:00 GMT"}],
        "_meta": {"page": 2, "max_results": 1, "total": 2},
        "_links": {},
    }

    mock_responses = [_make_mock_response(page_1), _make_mock_response(page_2)]

    with patch.object(downloader.opener, "open", side_effect=mock_responses):
        report = downloader.download_dataset(
            site="caltech",
            start_time="Tue, 01 Jan 2019 16:18:00 GMT",
            end_time="Tue, 31 Dec 2024 16:19:00 GMT",
        )

    assert report["download_status"] == "COMPLETE"
    assert report["pages_downloaded"] == 2
    assert report["actual_records_downloaded"] == 2


def test_download_retry_on_503_transient_error(downloader):
    """Downloader should retry on transient HTTP 503 and succeed on next attempt."""
    page_data = {
        "_items": [{"_id": "rec_001", "connectionTime": "Tue, 01 Jan 2019 17:00:00 GMT"}],
        "_meta": {"page": 1, "total": 1},
        "_links": {},
    }

    err_503 = HTTPError("https://ev.caltech.edu", 503, "Service Unavailable", {}, io.BytesIO(b"Busy"))
    good_resp = _make_mock_response(page_data)

    with patch.object(downloader.opener, "open", side_effect=[err_503, good_resp]):
        data = downloader.fetch_page("https://ev.caltech.edu/api/v1/sessions/caltech")

    assert len(data["_items"]) == 1
    assert data["_items"][0]["_id"] == "rec_001"


def test_download_retry_on_429_with_retry_after(downloader):
    """Downloader should respect Retry-After header on HTTP 429."""
    page_data = {
        "_items": [{"_id": "rec_001"}],
        "_meta": {"page": 1, "total": 1},
        "_links": {},
    }

    headers = {"Retry-After": "1"}
    err_429 = HTTPError("https://ev.caltech.edu", 429, "Too Many Requests", headers, io.BytesIO(b"Rate limit"))
    good_resp = _make_mock_response(page_data)

    with patch.object(downloader.opener, "open", side_effect=[err_429, good_resp]):
        with patch("time.sleep") as mock_sleep:
            data = downloader.fetch_page("https://ev.caltech.edu/api/v1/sessions/caltech")
            assert mock_sleep.called
            assert mock_sleep.call_args[0][0] <= downloader.max_backoff_sec

    assert len(data["_items"]) == 1


def test_download_fails_immediately_on_401_unauthorized(downloader):
    """Non-transient HTTP 401 Unauthorized must raise ACNDownloadError immediately without retrying."""
    err_401 = HTTPError("https://ev.caltech.edu", 401, "Unauthorized", {}, io.BytesIO(b"Bad token"))

    with patch.object(downloader.opener, "open", side_effect=err_401) as mock_call:
        with pytest.raises(ACNDownloadError, match="HTTP 401"):
            downloader.fetch_page("https://ev.caltech.edu/api/v1/sessions/caltech")
        assert mock_call.call_count == 1


def test_download_dataset_401_handling_sanitized_and_dataset_untouched(tmp_path):
    """HTTP 401 on dataset download must produce INCOMPLETE report, sanitize errors, and leave original file untouched."""
    # Place dummy original dataset in the raw directory
    orig_file = tmp_path / "acndata_sessions.json"
    original_payload = json.dumps({"_meta": {"original": True}, "_items": [{"_id": "orig_001"}]})
    orig_file.write_text(original_payload, encoding="utf-8")
    orig_stat_before = orig_file.stat()

    raw_token = "secret_user_api_key_xyz123"
    downloader = ACNDataDownloader(
        api_token=raw_token,
        raw_dir=tmp_path,
        max_retries=1,
    )

    err_401 = HTTPError(
        f"https://ev.caltech.edu/api/v1/sessions/caltech?token={raw_token}",
        401,
        "Unauthorized",
        {},
        io.BytesIO(b"Invalid token supplied"),
    )

    with patch.object(downloader.opener, "open", side_effect=err_401):
        report = downloader.download_dataset(site="caltech", start_time=None, end_time=None)

    # 1. Report correctly marks INCOMPLETE
    assert report["download_status"] == "INCOMPLETE"
    assert report["completeness_verified"] is False
    assert len(report["failures"]) == 1

    # 2. Sanitization check: raw token is NEVER exposed
    failure_err = report["failures"][0]["error"]
    assert "401" in failure_err
    assert raw_token not in failure_err

    # Check saved report file on disk
    report_text = (tmp_path / "download_report.json").read_text(encoding="utf-8")
    assert raw_token not in report_text

    # 3. Original dataset is completely unchanged
    assert orig_file.exists()
    assert orig_file.read_text(encoding="utf-8") == original_payload
    assert orig_file.stat().st_size == orig_stat_before.st_size


def test_download_checkpoint_and_resume(downloader, tmp_path):
    """Interrupted download should resume from cached chunk without re-fetching page 1."""
    page_1 = {
        "_items": [{"_id": "rec_001", "connectionTime": "Tue, 01 Jan 2019 17:00:00 GMT"}],
        "_meta": {"page": 1, "total": 2},
        "_links": {"next": {"href": "sessions/caltech?page=2"}},
    }
    page_2 = {
        "_items": [{"_id": "rec_002", "connectionTime": "Tue, 31 Dec 2024 16:00:00 GMT"}],
        "_meta": {"page": 2, "total": 2},
        "_links": {},
    }

    # First run fails on page 2
    err_net = URLError("Temporary connection failure")
    with patch.object(downloader.opener, "open", side_effect=[_make_mock_response(page_1), err_net]):
        report_1 = downloader.download_dataset(
            site="caltech",
            start_time="Tue, 01 Jan 2019 16:18:00 GMT",
            end_time="Tue, 31 Dec 2024 16:19:00 GMT",
        )

    assert report_1["download_status"] == "INCOMPLETE"
    assert report_1["pages_downloaded"] == 1
    assert (tmp_path / "chunks" / "page_00001.json").exists()

    # Second run should resume from page 2 and succeed
    with patch.object(downloader.opener, "open", side_effect=[_make_mock_response(page_2)]):
        report_2 = downloader.download_dataset(
            site="caltech",
            start_time="Tue, 01 Jan 2019 16:18:00 GMT",
            end_time="Tue, 31 Dec 2024 16:19:00 GMT",
        )

    assert report_2["download_status"] == "COMPLETE"
    assert report_2["pages_downloaded"] == 2
    assert report_2["actual_records_downloaded"] == 2


def test_download_stale_checkpoint_ignored(downloader, tmp_path):
    """A checkpoint for a different site or filter must be ignored and restarted."""
    stale_ckpt = {
        "site": "jpl",
        "query_filter": "old_query",
        "last_page_index": 5,
        "next_url": "https://ev.caltech.edu/api/v1/sessions/jpl?page=6",
        "total_records_so_far": 100,
        "chunk_files": [],
    }
    downloader.checkpoint_file.write_text(json.dumps(stale_ckpt), encoding="utf-8")

    page_1 = {
        "_items": [{"_id": "rec_caltech", "connectionTime": "Tue, 01 Jan 2019 17:00:00 GMT"}],
        "_meta": {"page": 1, "total": 1},
        "_links": {},
    }

    with patch.object(downloader.opener, "open", return_value=_make_mock_response(page_1)):
        report = downloader.download_dataset(site="caltech")

    assert report["site"] == "caltech"
    assert report["actual_records_downloaded"] == 1


def test_download_discontinuous_chunk_sequence_rejected(downloader, tmp_path):
    """If chunks are missing (e.g. page 1 and page 3 but no page 2), checkpoint is rejected."""
    chunks_dir = tmp_path / "chunks"
    chunks_dir.mkdir(parents=True, exist_ok=True)
    f1 = chunks_dir / "page_00001.json"
    f3 = chunks_dir / "page_00003.json"
    f1.write_text('{"_items": [{"_id": "1"}]}', encoding="utf-8")
    f3.write_text('{"_items": [{"_id": "3"}]}', encoding="utf-8")

    stale_ckpt = {
        "site": "caltech",
        "query_filter": "sort=connectionTime",
        "last_page_index": 3,
        "next_url": "https://ev.caltech.edu/api/v1/sessions/caltech?page=4",
        "total_records_so_far": 2,
        "chunk_files": [str(f1), str(f3)],
    }
    downloader.checkpoint_file.write_text(json.dumps(stale_ckpt), encoding="utf-8")

    page_fresh = {
        "_items": [{"_id": "rec_fresh", "connectionTime": "Tue, 01 Jan 2019 17:00:00 GMT"}],
        "_meta": {"page": 1, "total": 1},
        "_links": {},
    }

    with patch.object(downloader.opener, "open", return_value=_make_mock_response(page_fresh)):
        report = downloader.download_dataset(site="caltech", start_time=None, end_time=None)

    # Must start fresh from page 1
    assert report["pages_downloaded"] == 1
    assert report["actual_records_downloaded"] == 1


def test_download_circular_pagination_detected(downloader):
    """Circular next link pointing to an already visited URL must terminate pagination safely."""
    init_url = downloader.build_initial_url(site="caltech", start_time=None, end_time=None)
    init_href = init_url.replace(downloader.base_url + "/", "")

    page_1 = {
        "_items": [{"_id": "rec_001"}],
        "_meta": {"page": 1, "total": 5},
        "_links": {"next": {"href": "sessions/caltech?page=2"}},
    }
    page_2 = {
        "_items": [{"_id": "rec_002"}],
        "_meta": {"page": 2, "total": 5},
        # Next points back to initial URL that was already visited!
        "_links": {"next": {"href": init_href}},
    }

    with patch.object(downloader.opener, "open", side_effect=[_make_mock_response(page_1), _make_mock_response(page_2)]):
        report = downloader.download_dataset(site="caltech", start_time=None, end_time=None)

    assert report["download_status"] == "INCOMPLETE"
    assert any("Circular pagination detected" in f["error"] for f in report["failures"])


def test_download_pagination_link_escapes_host_detected(downloader):
    """Next link pointing to a foreign host must be blocked and recorded as a failure."""
    page_1 = {
        "_items": [{"_id": "rec_001"}],
        "_meta": {"page": 1, "total": 5},
        "_links": {"next": {"href": "https://attacker.org/sessions/caltech?page=2"}},
    }

    with patch.object(downloader.opener, "open", return_value=_make_mock_response(page_1)):
        report = downloader.download_dataset(site="caltech", start_time=None, end_time=None)

    assert report["download_status"] == "INCOMPLETE"
    assert any("escaped expected API host" in f["error"] for f in report["failures"])


def test_download_detects_inconsistent_meta_total(downloader):
    """If _meta.total changes mid-pagination, anomaly must be flagged."""
    page_1 = {
        "_items": [{"_id": "rec_001", "connectionTime": "Tue, 01 Jan 2019 17:00:00 GMT"}],
        "_meta": {"page": 1, "total": 10},
        "_links": {"next": {"href": "sessions/caltech?page=2"}},
    }
    page_2 = {
        "_items": [{"_id": "rec_002", "connectionTime": "Tue, 31 Dec 2024 16:00:00 GMT"}],
        "_meta": {"page": 2, "total": 20},
        "_links": {},
    }

    with patch.object(downloader.opener, "open", side_effect=[_make_mock_response(page_1), _make_mock_response(page_2)]):
        report = downloader.download_dataset(site="caltech", start_time=None, end_time=None)

    assert len(report["inconsistent_totals"]) == 1
    assert report["inconsistent_totals"][0]["previous_total"] == 10
    assert report["inconsistent_totals"][0]["new_total"] == 20
    assert report["download_status"] == "INCOMPLETE"


def test_download_detects_duplicate_records(downloader):
    """If an item _id appears on multiple pages, duplicate count must be recorded."""
    page_1 = {
        "_items": [{"_id": "duplicate_id", "connectionTime": "Tue, 01 Jan 2019 17:00:00 GMT"}],
        "_meta": {"page": 1, "total": 2},
        "_links": {"next": {"href": "sessions/caltech?page=2"}},
    }
    page_2 = {
        "_items": [{"_id": "duplicate_id", "connectionTime": "Tue, 31 Dec 2024 16:00:00 GMT"}],
        "_meta": {"page": 2, "total": 2},
        "_links": {},
    }

    with patch.object(downloader.opener, "open", side_effect=[_make_mock_response(page_1), _make_mock_response(page_2)]):
        report = downloader.download_dataset(site="caltech", start_time=None, end_time=None)

    assert report["duplicate_records_count"] == 1
    assert report["unique_records_count"] == 1
    assert report["actual_records_downloaded"] == 2


def test_download_completeness_flagged_incomplete_on_date_gap(downloader):
    """Dataset with missing end dates must be flagged INCOMPLETE."""
    page_1 = {
        "_items": [{"_id": "rec_001", "connectionTime": "Tue, 01 Jan 2019 17:00:00 GMT"}],
        "_meta": {"page": 1, "total": 1},
        "_links": {},
    }

    with patch.object(downloader.opener, "open", return_value=_make_mock_response(page_1)):
        report = downloader.download_dataset(
            site="caltech",
            start_time="Tue, 01 Jan 2019 16:18:00 GMT",
            end_time="Tue, 31 Dec 2024 16:19:00 GMT",
        )

    assert report["completeness_verified"] is False
    assert report["download_status"] == "INCOMPLETE"
    assert any("Date range does not span" in r for r in report["status_reasons"])


def test_download_never_overwrites_original_file(downloader, tmp_path):
    """The original raw dataset must never be overwritten."""
    original_file = tmp_path / "acndata_sessions.json"
    original_file.write_text("DO_NOT_TOUCH_ORIGINAL", encoding="utf-8")

    page_1 = {
        "_items": [{"_id": "rec_001", "connectionTime": "Tue, 01 Jan 2019 17:00:00 GMT"}],
        "_meta": {"page": 1, "total": 1},
        "_links": {},
    }

    with patch.object(downloader.opener, "open", return_value=_make_mock_response(page_1)):
        downloader.download_dataset(site="caltech", start_time=None, end_time=None)

    # Original file is preserved exactly
    assert original_file.read_text(encoding="utf-8") == "DO_NOT_TOUCH_ORIGINAL"
    # Candidate file is separate
    assert (tmp_path / "acndata_sessions_full_candidate.json").exists()


def test_candidate_compatible_with_extract_sessions(downloader, tmp_path):
    """The assembled candidate file must be strictly valid JSON and parseable by extract_sessions."""
    page_1 = {
        "_items": [
            {
                "_id": "5c412c1df9af8b12cb56c27c",
                "clusterID": "0039",
                "connectionTime": "Tue, 01 Jan 2019 17:41:45 GMT",
                "disconnectTime": "Tue, 01 Jan 2019 18:39:21 GMT",
                "doneChargingTime": "Tue, 01 Jan 2019 18:30:00 GMT",
                "kWhDelivered": 5.0,
                "sessionID": "test_sess_001",
                "siteID": "0002",
                "spaceID": "CA-512",
                "stationID": "2-39-138-566",
                "timezone": "America/Los_Angeles",
                "userID": "000000558",
                "userInputs": None,
            }
        ],
        "_meta": {"page": 1, "total": 1},
        "_links": {},
    }

    with patch.object(downloader.opener, "open", return_value=_make_mock_response(page_1)):
        downloader.download_dataset(site="caltech", start_time=None, end_time=None)

    candidate_path = tmp_path / "acndata_sessions_full_candidate.json"
    assert candidate_path.exists()

    # Ingestion extract should parse candidate with zero errors and allow_recovery=False (strict mode)
    sessions, meta = extract_sessions(str(candidate_path), allow_recovery=False)
    assert len(sessions) == 1
    assert sessions[0]["sessionID"] == "test_sess_001"
    assert meta["site"] == "caltech"


def test_token_never_exposed_in_exception(downloader):
    """Exception messages must never expose raw tokens."""
    token = "secret_key_9999999"
    downloader.api_token = token

    err = HTTPError(
        f"https://ev.caltech.edu/api/v1/sessions/caltech?token={token}",
        403,
        f"Forbidden with {token}",
        {},
        io.BytesIO(b"Forbidden"),
    )

    with patch.object(downloader.opener, "open", side_effect=err):
        with pytest.raises(ACNDownloadError) as exc_info:
            downloader.fetch_page("https://ev.caltech.edu/api/v1/sessions/caltech")

        err_str = str(exc_info.value)
        assert token not in err_str
        assert "sec...999" in err_str


def test_download_sort_ascending_default(downloader):
    """build_initial_url must default to sort=-connectionTime for ascending chronological ordering."""
    url = downloader.build_initial_url(site="caltech", start_time=None, end_time=None)
    assert "sort=-connectionTime" in url


def test_atomic_write_blocks_original_dataset(tmp_path):
    """_atomic_write_json must explicitly raise ACNDownloadError if targeted at acndata_sessions.json."""
    from ingestion.download import _atomic_write_json, ACNDownloadError
    target = tmp_path / "acndata_sessions.json"
    with pytest.raises(ACNDownloadError, match="Safety violation: refused to write"):
        _atomic_write_json(target, {"dummy": 1})


def test_downloader_refuses_candidate_named_original(tmp_path):
    """ACNDataDownloader must refuse to initialize if candidate_file points to the original dataset."""
    from ingestion.download import ACNDataDownloader
    with patch("ingestion.download.CANDIDATE_FILENAME", "acndata_sessions.json"):
        with pytest.raises(ValueError, match="Refusing to configure candidate_file"):
            ACNDataDownloader(raw_dir=tmp_path)


def test_download_halts_on_midstream_total_shift(downloader):
    """When API-reported total shifts mid-stream, download must halt immediately to avoid combining disparate sets."""
    page_1 = {
        "_items": [{"_id": "rec_001"}],
        "_meta": {"page": 1, "total": 100},
        "_links": {"next": {"href": "sessions/caltech?page=2"}},
    }
    # Page 2 reports total shifted to 105
    page_2 = {
        "_items": [{"_id": "rec_002"}],
        "_meta": {"page": 2, "total": 105},
        "_links": {"next": {"href": "sessions/caltech?page=3"}},
    }

    with patch.object(downloader.opener, "open", side_effect=[_make_mock_response(page_1), _make_mock_response(page_2)]):
        report = downloader.download_dataset(site="caltech", start_time=None, end_time=None)

    assert report["download_status"] == "INCOMPLETE"
    assert report["completeness_verified"] is False
    assert any("total shifted from 100 to 105" in f["error"] for f in report["failures"])
    assert len(report["inconsistent_totals"]) == 1


def test_download_report_contains_unverified_assumptions(downloader):
    """Completeness report must explicitly detail unverified assumptions."""
    page_1 = {
        "_items": [{"_id": "rec_001", "connectionTime": "Tue, 01 Jan 2019 17:41:45 GMT"}],
        "_meta": {"page": 1, "total": 1},
        "_links": {},
    }
    with patch.object(downloader.opener, "open", return_value=_make_mock_response(page_1)):
        report = downloader.download_dataset(site="caltech", start_time=None, end_time=None)

    assert "unverified_assumptions" in report
    assert isinstance(report["unverified_assumptions"], list)
    assert len(report["unverified_assumptions"]) >= 3
    assert any("sort=-connectionTime" in a for a in report["unverified_assumptions"])


def test_auth_header_construction_official_basic_auth():
    """Token must be encoded via HTTP Basic Auth with API token as username and empty password."""
    import base64
    from ingestion.download import ACNDataDownloader

    token = "test_custom_token_abcdef12345"
    downloader = ACNDataDownloader(api_token=token)
    header = downloader._build_auth_header()

    assert header is not None
    assert header.startswith("Basic ")
    encoded_part = header.split(" ", 1)[1]
    decoded = base64.b64decode(encoded_part).decode("utf-8")

    # Official documentation: token as username, blank password -> "token:"
    assert decoded.endswith(":")
    assert decoded == f"{token}:"
    assert decoded[:-1] == token


def test_auth_header_missing_or_empty_token():
    """When api_token is None or empty, _build_auth_header must return None."""
    from ingestion.download import ACNDataDownloader

    dl_none = ACNDataDownloader(api_token=None)
    dl_none.api_token = None
    assert dl_none._build_auth_header() is None

    dl_empty = ACNDataDownloader(api_token="")
    dl_empty.api_token = ""
    assert dl_empty._build_auth_header() is None


def test_fetch_page_omits_auth_header_when_no_token():
    """fetch_page must not attach an Authorization header when api_token is None."""
    from ingestion.download import ACNDataDownloader

    dl = ACNDataDownloader(api_token=None)
    dl.api_token = None

    captured_requests = []

    def mock_open(req, timeout):
        captured_requests.append(req)
        resp = MagicMock()
        resp.read.return_value = json.dumps({"_items": [], "_meta": {}}).encode("utf-8")
        resp.__enter__.return_value = resp
        return resp

    with patch.object(dl.opener, "open", side_effect=mock_open):
        dl.fetch_page("https://ev.caltech.edu/api/v1/sessions/caltech")

    assert len(captured_requests) == 1
    assert "Authorization" not in captured_requests[0].headers
    assert "authorization" not in captured_requests[0].headers


def test_sanitize_message_scrubs_token_and_basic_auth():
    """sanitize_message must redact raw tokens, base64 variants, and Basic auth headers."""
    import base64
    from ingestion.download import sanitize_message

    token = "secret_acn_token_987654321"
    b64 = base64.b64encode(f"{token}:".encode()).decode()

    raw_msg = f"Failed to connect with token={token} and header Basic {b64}"
    clean_msg = sanitize_message(raw_msg, token)

    assert token not in clean_msg
    assert b64 not in clean_msg
    assert "Basic [REDACTED]" in clean_msg


def test_diagnose_auth_config_missing_token(tmp_path):
    """diagnose_auth_config must report MISSING_TOKEN when no token is defined."""
    from ingestion.download import diagnose_auth_config

    empty_env = tmp_path / ".env"
    empty_env.write_text("# empty\n", encoding="utf-8")

    with patch.dict(os.environ, {}, clear=True):
        diag = diagnose_auth_config(empty_env)
        assert diag["status"] == "MISSING_TOKEN"
        assert diag["token_configured"] is False


def test_diagnose_auth_config_placeholder(tmp_path):
    """diagnose_auth_config must report PLACEHOLDER_TOKEN when token is a known placeholder."""
    from ingestion.download import diagnose_auth_config

    ph_env = tmp_path / ".env"
    ph_env.write_text("ACNDATA_API_TOKEN=your_actual_token_here\n", encoding="utf-8")

    with patch.dict(os.environ, {"ACNDATA_API_TOKEN": "your_actual_token_here"}, clear=True):
        diag = diagnose_auth_config(ph_env)
        assert diag["status"] == "PLACEHOLDER_TOKEN"
        assert diag["is_placeholder"] is True


def test_diagnose_auth_config_env_mismatch(tmp_path):
    """diagnose_auth_config must detect mismatch between file and process environment."""
    from ingestion.download import diagnose_auth_config

    mismatch_env = tmp_path / ".env"
    mismatch_env.write_text("ACNDATA_API_TOKEN=token_in_file_12345\n", encoding="utf-8")

    with patch.dict(os.environ, {"ACNDATA_API_TOKEN": "different_process_token_67890"}, clear=True):
        diag = diagnose_auth_config(mismatch_env)
        assert diag["status"] == "ENV_MISMATCH"
        assert diag["env_mismatch_detected"] is True


def test_diagnose_auth_config_valid_ready(tmp_path):
    """diagnose_auth_config must report CONFIGURED_AWAITING_SERVER_VALIDATION for valid config."""
    from ingestion.download import diagnose_auth_config

    valid_env = tmp_path / ".env"
    valid_env.write_text("ACNDATA_API_TOKEN=my_real_acn_token_xyz99\n", encoding="utf-8")

    with patch.dict(os.environ, {"ACNDATA_API_TOKEN": "my_real_acn_token_xyz99"}, clear=True):
        diag = diagnose_auth_config(valid_env)
        assert diag["status"] == "CONFIGURED_AWAITING_SERVER_VALIDATION"
        assert diag["basic_auth_encoding_valid"] is True
        assert diag["is_placeholder"] is False


def test_sanitize_url_eve_unencoded_pagination():
    """sanitize_url must encode spaces and quotes from Caltech Eve API next links."""
    from urllib.request import Request

    raw_url = (
        'https://ev.caltech.edu/api/v1/sessions/caltech?'
        'where=connectionTime>="Tue, 01 Jan 2019 16:18:00 GMT" and connectionTime<="Tue, 31 Dec 2024 16:19:00 GMT"'
        '&sort=-connectionTime&page=2'
    )
    clean_url = sanitize_url(raw_url)

    assert " " not in clean_url
    assert '"' not in clean_url
    assert "page=2" in clean_url
    assert "sort=-connectionTime" in clean_url

    # Must be accepted by urllib Request without raising control character ValueError
    req = Request(clean_url)
    assert req.selector.startswith("/api/v1/sessions/caltech?")
    assert " " not in req.selector


def test_sanitize_url_idempotent_on_encoded_urls():
    """sanitize_url must not double-encode already encoded query strings."""
    encoded_url = (
        "https://ev.caltech.edu/api/v1/sessions/caltech?"
        "where=connectionTime%3E%3D%22Tue%2C+01+Jan+2019+16%3A18%3A00+GMT%22&sort=-connectionTime"
    )
    result = sanitize_url(encoded_url)
    assert result == encoded_url

