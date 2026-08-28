#!/usr/bin/env python3
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
Shaded jar leak checker for Apache Fluss.

Verifies that uber-jars do not ship third-party classes at their original
package paths, where they can shadow a downstream application's own copy of the
same library at runtime.

Two leak shapes are detected:

1. Base-path leaks -- e.g. ``com/fasterxml/jackson/core/JsonToken.class``
   sitting next to the relocated copy because no ``<relocation>`` was
   configured for the bundling module.

2. Multi-Release JAR leaks -- the same classes under
   ``META-INF/versions/<n>/``. The Maven Shade Plugin relocates base-path
   classes but *not* MRJ entries, so these survive relocation and must be
   excluded by a shade ``<filter>`` instead.

A rule passes only when the forbidden prefix is absent AND its relocated
counterpart is present, or when the package is absent from the jar entirely.
Absent-and-not-relocated is reported as a failure: that combination is what a
silently discarded ``<relocations>`` block looks like, since a global MRJ
filter will strip the classes rather than relocate them.

Usage::

    # fail on any leak
    python3 tools/ci/check_shaded_jars.py path/to/*.jar

    # record a baseline, then compare a later build against it
    python3 tools/ci/check_shaded_jars.py --baseline before.json path/to/*.jar
    python3 tools/ci/check_shaded_jars.py --compare  before.json path/to/*.jar

Exit codes: 0 = clean, 1 = violations found, 2 = usage or I/O error.
"""

from __future__ import annotations

import argparse
import glob
import json
import re
import sys
import zipfile
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

# Entries of the form META-INF/versions/<n>/<real path>. The Shade Plugin does
# not rewrite these, which is the whole reason this checker exists.
MRJ_PREFIX = re.compile(r"^META-INF/versions/\d+/")

# Packages that must never appear at their original path in a Fluss uber-jar,
# each paired with a regex matching where the relocated copy should live.
#
# The relocated patterns are deliberately loose about the middle segment: Fluss
# uses several shading namespaces (org.apache.fluss.shaded.*,
# org.apache.fluss.fs.shaded.s3.*, org.apache.fluss.fs.shaded.hadoop3.*) and
# this checker only cares that the classes ended up somewhere under
# org/apache/fluss/, not which namespace was chosen.
RULES: Sequence[Tuple[str, str]] = (
    # the five relocated by fluss-fs-hadoop-shaded and fluss-fs-s3
    ("com/fasterxml/", r"^org/apache/fluss/.*/com/fasterxml/"),
    ("org/codehaus/", r"^org/apache/fluss/.*/org/codehaus/"),
    ("com/ctc/", r"^org/apache/fluss/.*/com/ctc/"),
    ("org/apache/htrace/", r"^org/apache/fluss/.*/org/apache/htrace/"),
    ("com/google/re2j/", r"^org/apache/fluss/.*/com/google/re2j/"),
    ("org/apache/commons/", r"^org/apache/fluss/.*/org/apache/commons/"),
    # the libraries Fluss ships pre-shaded; see the forbidden-import list in
    # AGENTS.md. An unshaded copy in an uber-jar defeats that shading.
    ("com/google/common/", r"^org/apache/fluss/shaded/guava\d*/com/google/common/"),
    ("io/netty/", r"^org/apache/fluss/shaded/netty\d*/io/netty/"),
    ("org/apache/arrow/", r"^org/apache/fluss/shaded/arrow/org/apache/arrow/"),
    ("org/apache/zookeeper/", r"^org/apache/fluss/shaded/zookeeper\d*/org/apache/zookeeper/"),
)

# Prefixes that are legitimately present unshaded.
#
# org/apache/hadoop is the notable one: it is deliberately never relocated
# anywhere in this repository. fluss-fs-hadoop-shaded relocates only re2j,
# htrace, fasterxml, codehaus and ctc, leaving Hadoop itself at its real
# package because the Hadoop FileSystem SPI resolves implementations by class
# name. Its entries are still counted and reported so that a change in the
# footprint is visible, but they never fail the check.
ALLOWED_PREFIXES: Sequence[str] = (
    "org/apache/fluss/",
    "org/apache/hadoop/",
    "com/amazonaws/",
    "java/",
    "javax/",
    "jdk/",
    "sun/",
)

# Reported alongside the rules so drift is visible in --compare, never fatal.
TRACKED_PREFIXES: Sequence[str] = ("org/apache/hadoop/",)

# Leaks that already exist on main and are out of scope for the change being
# validated. They are still detected and printed, but as WARN rather than FAIL,
# so the checker stays usable as a gate. --compare still fails if one of these
# grows. Entries are (jar filename prefix, forbidden package prefix).
#
# fluss-client bundles commons-lang3 at its original path, and the Flink
# connector uber-jars inherit it. #3960 relocated org.apache.commons in the
# S3/GS/Azure filesystem plugins only; the client was never covered. Separate
# pre-existing issue from #3553 / #4072.
KNOWN_EXCEPTIONS: Sequence[Tuple[str, str]] = (
    ("fluss-client", "org/apache/commons/"),
    ("fluss-flink-", "org/apache/commons/"),
)


def is_known_exception(jar_path: str, prefix: str) -> bool:
    name = jar_path.rsplit("/", 1)[-1]
    return any(
        name.startswith(jar_prefix) and prefix == pkg
        for jar_prefix, pkg in KNOWN_EXCEPTIONS
    )


class JarReport:
    """Per-jar scan result: leak counts, relocated counts, sample entries."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.total_entries = 0
        self.total_classes = 0
        # forbidden prefix -> counts
        self.leaked: Dict[str, int] = {p: 0 for p, _ in RULES}
        self.leaked_mrj: Dict[str, int] = {p: 0 for p, _ in RULES}
        self.relocated: Dict[str, int] = {p: 0 for p, _ in RULES}
        self.tracked: Dict[str, int] = {p: 0 for p in TRACKED_PREFIXES}
        # forbidden prefix -> first few offending entry names
        self.samples: Dict[str, List[str]] = {p: [] for p, _ in RULES}
        # non-jackson MRJ entries, so we can prove unrelated MRJ content survived
        self.mrj_other = 0

    def to_dict(self) -> dict:
        return {
            "path": self.path,
            "total_entries": self.total_entries,
            "total_classes": self.total_classes,
            "leaked": self.leaked,
            "leaked_mrj": self.leaked_mrj,
            "relocated": self.relocated,
            "tracked": self.tracked,
            "mrj_other": self.mrj_other,
        }

    def violations(self) -> Tuple[List[str], List[str]]:
        """Return (fatal, warnings) as human-readable reasons.

        Warnings are leaks listed in KNOWN_EXCEPTIONS: real, but pre-existing
        and out of scope, so they are reported without failing the gate.
        """
        fatal: List[str] = []
        warnings: List[str] = []
        for prefix, _ in RULES:
            sink = warnings if is_known_exception(self.path, prefix) else fatal
            base = self.leaked[prefix]
            mrj = self.leaked_mrj[prefix]
            if base:
                sink.append(
                    "{} unshaded entries at {} (base path)".format(base, prefix)
                )
            if mrj:
                sink.append(
                    "{} unshaded entries at META-INF/versions/*/{}".format(mrj, prefix)
                )
        return fatal, warnings


def _classify(name: str, report: JarReport, relocated_res: Sequence[re.Pattern]) -> None:
    mrj_match = MRJ_PREFIX.match(name)
    logical = name[mrj_match.end():] if mrj_match else name

    if mrj_match and logical:
        if not any(logical.startswith(p) for p, _ in RULES):
            report.mrj_other += 1

    for prefix in TRACKED_PREFIXES:
        if logical.startswith(prefix):
            report.tracked[prefix] += 1

    for idx, (prefix, _) in enumerate(RULES):
        if logical.startswith(prefix):
            bucket = report.leaked_mrj if mrj_match else report.leaked
            bucket[prefix] += 1
            if len(report.samples[prefix]) < 5:
                report.samples[prefix].append(name)
            return
        if relocated_res[idx].match(logical):
            report.relocated[prefix] += 1
            return


def audit_packages(path: str, depth: int = 3) -> List[Tuple[str, int]]:
    """Every non-Fluss, non-JDK package shipped in the jar, largest first.

    Discovery aid for the repo-wide sweep: RULES only covers packages we
    already know about, so this answers "what else is in here". Entries are
    grouped to `depth` path segments, and relocated classes under
    org/apache/fluss are folded away since those are the shaded copies.
    """
    counts: Dict[str, int] = {}
    jdk = ("java/", "javax/", "jdk/", "sun/", "META-INF/")
    with zipfile.ZipFile(path) as zf:
        for name in zf.namelist():
            if not name.endswith(".class"):
                continue
            logical = MRJ_PREFIX.sub("", name)
            if logical.startswith("org/apache/fluss/") or logical.startswith(jdk):
                continue
            parts = logical.split("/")
            if len(parts) <= 1:
                key = "(default package)"
            else:
                key = "/".join(parts[: min(depth, len(parts) - 1)])
            counts[key] = counts.get(key, 0) + 1
    return sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))


def scan_jar(path: str) -> JarReport:
    report = JarReport(path)
    relocated_res = [re.compile(pat) for _, pat in RULES]
    with zipfile.ZipFile(path) as zf:
        for name in zf.namelist():
            report.total_entries += 1
            if not name.endswith(".class"):
                continue
            report.total_classes += 1
            _classify(name, report, relocated_res)
    return report


def format_report(report: JarReport) -> str:
    lines = [
        "",
        report.path,
        "  {} entries, {} classes".format(report.total_entries, report.total_classes),
    ]
    header = "  {:<22} {:>8} {:>8} {:>10}".format(
        "package", "leaked", "mrj", "relocated"
    )
    lines.append(header)
    lines.append("  " + "-" * (len(header) - 2))
    for prefix, _ in RULES:
        base = report.leaked[prefix]
        mrj = report.leaked_mrj[prefix]
        reloc = report.relocated[prefix]
        if not (base or mrj or reloc):
            continue
        flag = "  LEAK" if (base or mrj) else ""
        lines.append(
            "  {:<22} {:>8} {:>8} {:>10}{}".format(
                prefix.rstrip("/"), base, mrj, reloc, flag
            )
        )
    for prefix in TRACKED_PREFIXES:
        lines.append(
            "  {:<22} {:>8} {:>8} {:>10}  (tracked, never fatal)".format(
                prefix.rstrip("/"), report.tracked[prefix], "-", "-"
            )
        )
    lines.append(
        "  {:<22} {:>8}".format("other MRJ entries", report.mrj_other)
    )
    for prefix, _ in RULES:
        for sample in report.samples[prefix]:
            lines.append("    e.g. {}".format(sample))
    return "\n".join(lines)


def compare_reports(
    baseline: Sequence[dict], current: Sequence[JarReport]
) -> Tuple[List[str], List[str]]:
    """Diff current reports against a baseline. Returns (regressions, notes)."""
    by_name = {}
    for entry in baseline:
        by_name[entry["path"].rsplit("/", 1)[-1]] = entry

    regressions: List[str] = []
    notes: List[str] = []
    for report in current:
        key = report.path.rsplit("/", 1)[-1]
        before = by_name.get(key)
        if before is None:
            notes.append("{}: no baseline entry, skipped comparison".format(key))
            continue
        for prefix, _ in RULES:
            for field, label in (("leaked", "base"), ("leaked_mrj", "mrj")):
                was = before[field].get(prefix, 0)
                now = getattr(report, field)[prefix]
                if now > was:
                    regressions.append(
                        "{}: {} {} leaks rose {} -> {}".format(
                            key, prefix, label, was, now
                        )
                    )
                elif now < was:
                    notes.append(
                        "{}: {} {} leaks fell {} -> {}".format(
                            key, prefix, label, was, now
                        )
                    )
            # A base-path leak that disappeared should reappear as relocated
            # classes. If it did not, the classes were stripped by a filter
            # rather than rewritten -- the signature of a <relocations> block
            # that Maven silently discarded. Losing the classes outright breaks
            # the bundled library at runtime, so this is fatal.
            was_leaked = before["leaked"].get(prefix, 0)
            was_reloc = before["relocated"].get(prefix, 0)
            now_reloc = report.relocated[prefix]
            if was_leaked > 0 and report.leaked[prefix] == 0:
                gained = now_reloc - was_reloc
                if gained < was_leaked * 0.9:
                    regressions.append(
                        "{}: {} lost {} unshaded entries but gained only {} "
                        "relocated -- classes were stripped, not relocated".format(
                            key, prefix, was_leaked, gained
                        )
                    )
        for prefix in TRACKED_PREFIXES:
            was = before.get("tracked", {}).get(prefix, 0)
            now = report.tracked[prefix]
            if was != now:
                notes.append(
                    "{}: {} count changed {} -> {} (tracked, not fatal)".format(
                        key, prefix, was, now
                    )
                )
        was_mrj = before.get("mrj_other", 0)
        if report.mrj_other < was_mrj:
            regressions.append(
                "{}: unrelated MRJ entries dropped {} -> {} "
                "(a filter is too broad)".format(key, was_mrj, report.mrj_other)
            )
    return regressions, notes


def expand(patterns: Iterable[str]) -> List[str]:
    paths: List[str] = []
    for pattern in patterns:
        matches = sorted(glob.glob(pattern))
        if matches:
            paths.extend(matches)
        else:
            paths.append(pattern)
    # a shaded build leaves original-* and dependency-reduced artifacts around
    return [p for p in paths if not p.rsplit("/", 1)[-1].startswith("original-")]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check Fluss uber-jars for unshaded third-party classes."
    )
    parser.add_argument("jars", nargs="+", help="jar paths or globs")
    parser.add_argument(
        "--baseline", metavar="FILE", help="write scan results to FILE and exit 0"
    )
    parser.add_argument(
        "--compare", metavar="FILE", help="compare against a baseline written earlier"
    )
    parser.add_argument("--json", action="store_true", help="emit JSON to stdout")
    parser.add_argument(
        "--audit",
        action="store_true",
        help="list every unshaded third-party package per jar and exit 0; "
        "discovery mode, does not gate",
    )
    parser.add_argument(
        "--audit-min",
        type=int,
        default=1,
        metavar="N",
        help="with --audit, hide packages with fewer than N classes",
    )
    args = parser.parse_args()

    paths = expand(args.jars)
    if not paths:
        print("no jars matched", file=sys.stderr)
        return 2

    if args.audit:
        for path in paths:
            try:
                packages = audit_packages(path)
            except (OSError, zipfile.BadZipFile) as err:
                print("cannot read {}: {}".format(path, err), file=sys.stderr)
                continue
            shown = [(p, n) for p, n in packages if n >= args.audit_min]
            total = sum(n for _, n in packages)
            print("\n{}  ({} unshaded third-party classes)".format(path, total))
            if not shown:
                print("  none")
            for package, count in shown:
                print("  {:>7}  {}".format(count, package))
        return 0

    reports = []
    for path in paths:
        try:
            reports.append(scan_jar(path))
        except (OSError, zipfile.BadZipFile) as err:
            print("cannot read {}: {}".format(path, err), file=sys.stderr)
            return 2

    if args.json:
        print(json.dumps([r.to_dict() for r in reports], indent=2))
    else:
        for report in reports:
            print(format_report(report))

    if args.baseline:
        with open(args.baseline, "w") as handle:
            json.dump([r.to_dict() for r in reports], handle, indent=2)
        print("\nbaseline written to {}".format(args.baseline))
        return 0

    failed = False

    if args.compare:
        try:
            with open(args.compare) as handle:
                baseline = json.load(handle)
        except (OSError, ValueError) as err:
            print("cannot read baseline: {}".format(err), file=sys.stderr)
            return 2
        regressions, notes = compare_reports(baseline, reports)
        print("\n--- comparison against {} ---".format(args.compare))
        for note in notes:
            print("  ok   {}".format(note))
        for regression in regressions:
            print("  FAIL {}".format(regression))
        if not notes and not regressions:
            print("  no differences")
        failed = failed or bool(regressions)

    print("\n--- leak check ---")
    for report in reports:
        fatal, warnings = report.violations()
        name = report.path.rsplit("/", 1)[-1]
        if fatal:
            failed = True
            print("  FAIL {}".format(name))
            for problem in fatal:
                print("       {}".format(problem))
        else:
            print("  ok   {}".format(name))
        for problem in warnings:
            print("  WARN {}: {} (known pre-existing, not gating)".format(name, problem))

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
