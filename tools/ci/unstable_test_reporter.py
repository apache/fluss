################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""
Unstable Test Reporter for Apache Fluss CI.

Analyzes failed GitHub Actions workflow runs on the main branch,
parses Maven surefire test failure logs, and creates or updates
GitHub issues for unstable (flaky) tests.
"""

from __future__ import annotations

import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple
from urllib.request import Request, urlopen
import urllib.request as urllib_request
from urllib.error import HTTPError, URLError
from urllib.parse import quote
import json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REPO_OWNER = "apache"
REPO_NAME = "fluss"
GITHUB_API = "https://api.github.com"
WORKFLOW_FILES = ["ci.yaml", "nightly.yaml"]
ISSUE_TITLE_PREFIX = "[test] Unstable test "
MAX_STACK_TRACE_LINES = 30
DEFAULT_LOOKBACK_HOURS = 28
MAX_NEW_ISSUES = 5

# Regex patterns
TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z (.*)$")
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

# Maven surefire individual test failure line, e.g.:
# [ERROR] org.apache.fluss...Class.method(Type)[1]  Time elapsed: 1.23 s  <<< FAILURE!
# or: Error:  org.apache.fluss...Class.method  Time elapsed: 1.23 s  <<< FAILURE!
TEST_FAILURE_RE = re.compile(
    r"^(?:\[ERROR\]|Error:)\s+(\S+)\.(\w+)(?:\(.*?\))?(?:\[.*?\])?\s+.*<<<\s+(FAILURE|ERROR)!"
)

# Stack trace continuation patterns
STACK_LINE_RE = re.compile(
    r"^(?:\tat |Caused by: |\t\.\.\. \d+ more|\tSuppressed: |[\w.$]+(?:Exception|Error|Throwable|Failure))"
)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class TestFailure:
    class_name: str  # fully qualified, e.g. org.apache.fluss...ClassName
    method_name: str  # e.g. testSomething
    simple_class_name: str  # e.g. ClassName
    error_message: str  # header line + stack trace
    job_url: str
    run_id: int
    job_id: int


# ---------------------------------------------------------------------------
# GitHub API helpers
# ---------------------------------------------------------------------------


class GitHubAPI:
    """Thin wrapper around GitHub REST API using urllib."""

    def __init__(self, token: str):
        self.token = token
        self.base = GITHUB_API
        self._search_last_call = 0.0  # rate-limit search API

    def _request(
        self, method: str, url: str, body: Optional[dict] = None, max_retries: int = 3
    ) -> dict | bytes | list:
        headers = {
            "Authorization": f"token {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        data = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(body).encode()

        for attempt in range(max_retries):
            try:
                req = Request(url, data=data, headers=headers, method=method)
                with urlopen(req, timeout=60) as resp:
                    raw = resp.read()
                    content_type = resp.headers.get("Content-Type", "")
                    if "application/json" in content_type:
                        return json.loads(raw)
                    return raw
            except HTTPError as e:
                if e.code == 403:
                    # Rate limit or permission error
                    retry_after = e.headers.get("Retry-After")
                    rate_remaining = e.headers.get("X-RateLimit-Remaining")
                    if retry_after or rate_remaining == "0":
                        wait = int(retry_after) if retry_after else 60
                        log.warning("Rate limited. Waiting %d seconds...", wait)
                        time.sleep(wait)
                        continue
                    body_text = e.read().decode(errors="replace")
                    log.error("Permission error (403): %s", body_text)
                    raise
                elif e.code == 404:
                    raise
                elif e.code == 422:
                    body_text = e.read().decode(errors="replace")
                    log.error("Unprocessable entity (422): %s", body_text)
                    raise
                elif e.code >= 500:
                    if attempt < max_retries - 1:
                        wait = 2 ** (attempt + 1)
                        log.warning(
                            "Server error %d, retrying in %ds...", e.code, wait
                        )
                        time.sleep(wait)
                        continue
                    raise
                else:
                    raise
            except URLError:
                if attempt < max_retries - 1:
                    wait = 2 ** (attempt + 1)
                    log.warning("Network error, retrying in %ds...", wait)
                    time.sleep(wait)
                    continue
                raise
        return {}

    def get(self, path: str) -> dict | list:
        url = f"{self.base}{path}" if path.startswith("/") else path
        return self._request("GET", url)

    def post(self, path: str, body: dict) -> dict:
        url = f"{self.base}{path}" if path.startswith("/") else path
        return self._request("POST", url, body=body)

    def search_issues(self, query: str) -> list:
        # Rate-limit search API to avoid secondary limits
        elapsed = time.time() - self._search_last_call
        if elapsed < 2:
            time.sleep(2 - elapsed)
        self._search_last_call = time.time()
        encoded = quote(query)
        resp = self.get(f"/search/issues?q={encoded}&per_page=30")
        return resp.get("items", []) if isinstance(resp, dict) else []

    def download_log(self, job_id: int) -> Optional[str]:
        """Download job log. Returns log text or None if unavailable.

        The GitHub API responds with a 302 redirect to a storage URL.
        We must NOT forward the Authorization header to the redirect target,
        so we handle the redirect manually.
        """
        url = f"{self.base}/repos/{REPO_OWNER}/{REPO_NAME}/actions/jobs/{job_id}/logs"
        headers = {
            "Authorization": f"token {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        try:
            # Build an opener that does NOT auto-follow redirects
            class NoRedirect(urllib_request.HTTPRedirectHandler):
                def redirect_request(self, req, fp, code, msg, headers, newurl):
                    return None  # stop auto-redirect

            opener = urllib_request.build_opener(NoRedirect)
            req = Request(url, headers=headers, method="GET")
            try:
                with opener.open(req, timeout=120) as resp:
                    return resp.read().decode(errors="replace")
            except HTTPError as e:
                if e.code in (301, 302, 303, 307, 308):
                    redirect_url = e.headers.get("Location")
                    if redirect_url:
                        # Follow redirect WITHOUT auth header
                        req2 = Request(redirect_url, method="GET")
                        with urlopen(req2, timeout=120) as resp2:
                            return resp2.read().decode(errors="replace")
                elif e.code == 404:
                    log.warning("Log for job %d not available (404)", job_id)
                    return None
                raise
        except HTTPError as e:
            if e.code == 404:
                log.warning("Log for job %d not available (404)", job_id)
                return None
            raise
        except URLError as e:
            log.warning("Failed to download log for job %d: %s", job_id, e)
            return None


# ---------------------------------------------------------------------------
# Fetching workflow runs and jobs
# ---------------------------------------------------------------------------


def get_failed_runs(
    api: GitHubAPI, workflow_file: str, since: datetime
) -> List[dict]:
    """Get failed workflow runs on main branch since the given time."""
    path = (
        f"/repos/{REPO_OWNER}/{REPO_NAME}/actions/workflows/{workflow_file}/runs"
        f"?branch=main&status=failure&per_page=100"
    )
    resp = api.get(path)
    runs = resp.get("workflow_runs", []) if isinstance(resp, dict) else []

    filtered = []
    for run in runs:
        created = datetime.fromisoformat(run["created_at"].replace("Z", "+00:00"))
        if created >= since:
            filtered.append(run)
    log.info(
        "Workflow %s: found %d failed runs in lookback window",
        workflow_file,
        len(filtered),
    )
    return filtered


def get_failed_jobs(api: GitHubAPI, run_id: int) -> List[dict]:
    """Get failed jobs for a given workflow run."""
    path = f"/repos/{REPO_OWNER}/{REPO_NAME}/actions/runs/{run_id}/jobs?per_page=100"
    resp = api.get(path)
    jobs = resp.get("jobs", []) if isinstance(resp, dict) else []
    return [j for j in jobs if j.get("conclusion") == "failure"]


# ---------------------------------------------------------------------------
# Log parsing
# ---------------------------------------------------------------------------


def strip_timestamp(line: str) -> str:
    m = TIMESTAMP_RE.match(line)
    return m.group(1) if m else line


def strip_ansi(text: str) -> str:
    return ANSI_RE.sub("", text)


def parse_test_failures(
    log_text: str, job_url: str, run_id: int, job_id: int
) -> List[TestFailure]:
    """Parse Maven surefire test failures from a GitHub Actions job log."""
    lines = strip_ansi(log_text).splitlines()
    lines = [strip_timestamp(l) for l in lines]

    failures: List[TestFailure] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        m = TEST_FAILURE_RE.match(line)
        if not m:
            i += 1
            continue

        fq_class = m.group(1)  # fully qualified class (may include method prefix parts)
        method = m.group(2)

        # fq_class is everything before the last .method, e.g.:
        # "org.apache.fluss.lake.paimon.flink.FlinkUnionReadLogTableITCase"
        simple_class = fq_class.rsplit(".", 1)[-1] if "." in fq_class else fq_class

        # Collect the error header line
        error_lines = [line]
        i += 1

        # Collect stack trace
        while i < len(lines):
            sline = lines[i]
            # Stack trace lines: exception names, "at" frames, "Caused by:", "... N more", "Suppressed:", or indented lines
            if (
                STACK_LINE_RE.match(sline)
                or sline.startswith("\t")
                or (sline and sline[0] == " " and len(sline) > 1 and sline.strip().startswith("at "))
            ):
                error_lines.append(sline)
                i += 1
            else:
                break

        # Truncate if too long
        if len(error_lines) > MAX_STACK_TRACE_LINES:
            error_lines = error_lines[:MAX_STACK_TRACE_LINES]
            error_lines.append("... (truncated)")

        failures.append(
            TestFailure(
                class_name=fq_class,
                method_name=method,
                simple_class_name=simple_class,
                error_message="\n".join(error_lines),
                job_url=job_url,
                run_id=run_id,
                job_id=job_id,
            )
        )

    return failures


# ---------------------------------------------------------------------------
# Issue management
# ---------------------------------------------------------------------------


def build_issue_title(simple_class: str, method: str) -> str:
    return f"{ISSUE_TITLE_PREFIX}{simple_class}.{method}"


def build_issue_body(job_url: str, error_message: str) -> str:
    return f"{job_url}\n\n```\n{error_message}\n```\n"


def find_existing_issue(
    api: GitHubAPI, simple_class: str, method: str
) -> Optional[int]:
    """Search for an existing open issue for this test failure. Returns issue number or None."""
    title = build_issue_title(simple_class, method)
    query = (
        f"repo:{REPO_OWNER}/{REPO_NAME} is:issue is:open "
        f'in:title "{ISSUE_TITLE_PREFIX}{simple_class}.{method}"'
    )
    items = api.search_issues(query)
    for item in items:
        if item.get("title", "").strip() == title:
            return item["number"]
    return None


def is_already_commented(
    api: GitHubAPI, issue_number: int, job_url: str
) -> bool:
    """Check if a comment with this job_url already exists on the issue."""
    path = (
        f"/repos/{REPO_OWNER}/{REPO_NAME}/issues/{issue_number}/comments"
        f"?per_page=100&sort=created&direction=desc"
    )
    try:
        comments = api.get(path)
        if not isinstance(comments, list):
            return False
        for c in comments:
            if job_url in c.get("body", ""):
                return True
    except Exception as e:
        log.warning("Failed to check comments on issue #%d: %s", issue_number, e)
    return False


def is_already_in_issue_body(
    api: GitHubAPI, issue_number: int, job_url: str
) -> bool:
    """Check if the job_url already exists in the issue body."""
    path = f"/repos/{REPO_OWNER}/{REPO_NAME}/issues/{issue_number}"
    try:
        issue = api.get(path)
        if isinstance(issue, dict):
            return job_url in issue.get("body", "")
    except Exception as e:
        log.warning("Failed to fetch issue #%d: %s", issue_number, e)
    return False


def create_issue(
    api: GitHubAPI, title: str, body: str, dry_run: bool = False
) -> Optional[int]:
    if dry_run:
        log.info("[DRY RUN] Would create issue: %s", title)
        return None
    try:
        resp = api.post(
            f"/repos/{REPO_OWNER}/{REPO_NAME}/issues",
            {"title": title, "body": body},
        )
        number = resp.get("number")
        log.info("Created issue #%s: %s", number, title)
        return number
    except HTTPError as e:
        log.error("Failed to create issue '%s': HTTP %d", title, e.code)
        return None


def add_comment(
    api: GitHubAPI, issue_number: int, body: str, dry_run: bool = False
) -> None:
    if dry_run:
        log.info("[DRY RUN] Would comment on issue #%d", issue_number)
        return
    try:
        api.post(
            f"/repos/{REPO_OWNER}/{REPO_NAME}/issues/{issue_number}/comments",
            {"body": body},
        )
        log.info("Added comment to issue #%d", issue_number)
    except HTTPError as e:
        log.error("Failed to comment on issue #%d: HTTP %d", issue_number, e.code)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        log.error("GITHUB_TOKEN environment variable is required")
        sys.exit(1)

    lookback_hours = int(os.environ.get("LOOKBACK_HOURS", str(DEFAULT_LOOKBACK_HOURS)))
    dry_run = os.environ.get("DRY_RUN", "false").lower() in ("true", "1", "yes")

    if dry_run:
        log.info("Running in DRY RUN mode - no issues will be created or commented on")

    api = GitHubAPI(token)
    since = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    log.info("Looking back %d hours (since %s)", lookback_hours, since.isoformat())

    # Track stats
    total_runs = 0
    total_failures = 0
    issues_created = 0
    comments_added = 0

    # Dedup within this execution
    seen: set = set()

    # Phase 1: Collect all actions (create issue or add comment)
    # Each entry: ("create", title, body) or ("comment", issue_number, body)
    actions: list = []

    for wf_file in WORKFLOW_FILES:
        try:
            runs = get_failed_runs(api, wf_file, since)
        except Exception as e:
            log.error("Failed to fetch runs for %s: %s", wf_file, e)
            continue

        for run in runs:
            total_runs += 1
            run_id = run["id"]
            log.info(
                "Processing run #%d (workflow=%s, created=%s)",
                run_id,
                wf_file,
                run["created_at"],
            )

            try:
                jobs = get_failed_jobs(api, run_id)
            except Exception as e:
                log.error("Failed to fetch jobs for run %d: %s", run_id, e)
                continue

            for job in jobs:
                job_id = job["id"]
                job_url = job.get("html_url", "")
                job_name = job.get("name", "")
                log.info("  Processing job: %s (id=%d)", job_name, job_id)

                log_text = api.download_log(job_id)
                if not log_text:
                    log.warning("  No log available for job %d, skipping", job_id)
                    continue

                failures = parse_test_failures(log_text, job_url, run_id, job_id)
                if not failures:
                    log.info("  No test failures found in job log")
                    continue

                log.info("  Found %d test failure(s)", len(failures))

                for failure in failures:
                    key = (failure.simple_class_name, failure.method_name)
                    if key in seen:
                        log.info(
                            "  Skipping duplicate: %s.%s",
                            failure.simple_class_name,
                            failure.method_name,
                        )
                        continue
                    seen.add(key)
                    total_failures += 1

                    title = build_issue_title(
                        failure.simple_class_name, failure.method_name
                    )
                    body = build_issue_body(failure.job_url, failure.error_message)

                    try:
                        existing = find_existing_issue(
                            api, failure.simple_class_name, failure.method_name
                        )
                    except Exception as e:
                        log.error("Failed to search issues for %s: %s", title, e)
                        continue

                    if existing:
                        # Check if already commented with this job URL
                        if is_already_commented(
                            api, existing, failure.job_url
                        ) or is_already_in_issue_body(api, existing, failure.job_url):
                            log.info(
                                "  Already reported on issue #%d for %s",
                                existing,
                                failure.job_url,
                            )
                            continue
                        actions.append(("comment", existing, body))
                    else:
                        actions.append(("create", title, body))

    # Phase 2: Rate limit check and execute actions
    new_issue_count = sum(1 for a in actions if a[0] == "create")
    max_new_issues = int(os.environ.get("MAX_NEW_ISSUES", str(MAX_NEW_ISSUES)))

    if new_issue_count > max_new_issues:
        log.error(
            "Rate limit exceeded: would create %d new issues (max %d). "
            "Skipping all issue creation and commenting. "
            "The issues that would have been created:",
            new_issue_count,
            max_new_issues,
        )
        for action in actions:
            if action[0] == "create":
                log.error("  - %s", action[1])
            else:
                log.error("  - Comment on issue #%d", action[1])
        log.error(
            "This likely indicates a systemic CI problem rather than "
            "individual flaky tests. Please investigate manually."
        )
        sys.exit(1)

    for action in actions:
        if action[0] == "create":
            create_issue(api, action[1], action[2], dry_run)
            issues_created += 1
        else:
            add_comment(api, action[1], action[2], dry_run)
            comments_added += 1

    log.info(
        "Done. Processed %d run(s), found %d unique failure(s), "
        "created %d issue(s), added %d comment(s).",
        total_runs,
        total_failures,
        issues_created,
        comments_added,
    )


if __name__ == "__main__":
    main()
