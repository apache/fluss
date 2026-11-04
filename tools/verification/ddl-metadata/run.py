# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Reproduce bounded DDL admission and metadata protocol checks with TLC."""

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import time
from pathlib import Path

TLC_SHA256 = "936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"
GATE_DEFAULTS = dict(Scenario="same-table", ProtectDatabase=True,
                     ProtectTable=True, Cleanup=True)
PROTOCOL_DEFAULTS = dict(Actors={"d", "r", "R", "s", "S"}, Target="table",
                         Mode="safe", Focus="normal", CachedInput=False,
                         VerifyInput=True, AllowFailover=False, AllowReconnect=False)


TREE_DEFAULTS = dict(Tables={"t"}, Parts={"p", "q"}, Strategy="withdraw",
                     Events=True, AllowUnknown=True, AllowFailover=True,
                     CheckEpoch=True, WaitForKnown=True, MaintainCounts=True,
                     MissingAssignment=False, GuardUnknown=True)
PROFILES = {
    "DdlAdmission": (GATE_DEFAULTS,
        ["Exclusive", "Accounting", "NoEmptyEntries", "EntryBound"],
        ["Refines", "Progress", "EventuallyEmpty"]),
    "DdlProtocol": (PROTOCOL_DEFAULTS,
        ["NoWrongIncarnation", "Resources", "NoKnownStaleExternal"],
        ["Refines", "Progress"]),
    "DdlTree": (TREE_DEFAULTS,
        ["PublishedHasSchema", "ParentWhileKnown", "CountsExact", "CountEntriesExact",
         "CountEntryBound", "NoStartupFailure", "NoEventFailure", "NoOldEpochWrite", "Resources"],
        ["Refines", "Progress", "RecoveryProgress"]),
}


def case(model, overrides=None, expected=None, witness=False,
         extra_properties=(), omit_invariants=()):
    """Keep weakening controls and reachability witnesses separate from passes."""
    defaults, checks, progress = PROFILES[model]
    constants = dict(defaults)
    constants.update(overrides or {})
    invariants = [name for name in checks if name not in omit_invariants]
    properties = list(progress) + list(extra_properties)
    if expected:
        invariants = [expected] + [name for name in invariants if name != expected]
        if not witness:
            properties.remove("Refines")
    return dict(model=model, constants=constants, invariants=invariants,
                properties=properties, expected=expected, witness=witness)


CASES = {
    "admission-same-table": case("DdlAdmission"),
    "admission-other-table": case("DdlAdmission", dict(Scenario="other-table")),
    "admission-other-database": case("DdlAdmission", dict(Scenario="other-database")),
    "admission-no-parent-check": case("DdlAdmission", dict(ProtectDatabase=False), "Exclusive"),
    "admission-no-table-check": case("DdlAdmission", dict(ProtectTable=False), "Exclusive"),
    "admission-no-cleanup": case("DdlAdmission", dict(Cleanup=False), "NoEmptyEntries"),
    "admission-parallel": case("DdlAdmission", dict(Scenario="other-table"), "NeverParallelTables", True),
    "admission-reallocate": case("DdlAdmission", expected="NeverReallocated", witness=True),
    "metadata-table": case("DdlProtocol"),
    "metadata-partition": case("DdlProtocol", dict(Target="partition")),
    "metadata-failover": case("DdlProtocol", dict(AllowFailover=True)),
    "metadata-reconnect": case("DdlProtocol", dict(AllowReconnect=True)),
    "metadata-cached-table": case("DdlProtocol", dict(CachedInput=True)),
    "metadata-current-name": case("DdlProtocol", dict(Focus="current")),
    "metadata-create-race": case("DdlProtocol", dict(Actors={"r", "R", "x"})),
    "metadata-stale-external": case("DdlProtocol", dict(Actors={"d"}, Focus="external", AllowFailover=True)),
    "metadata-shared-ddl": case("DdlProtocol", dict(Mode="shared"), "NoWrongIncarnation"),
    "metadata-no-epoch": case("DdlProtocol", dict(Mode="no-epoch", AllowFailover=True), "NoWrongIncarnation"),
    "metadata-rebound-session": case("DdlProtocol", dict(Mode="rebind", AllowReconnect=True), "NoWrongIncarnation"),
    "metadata-unchecked-cache": case("DdlProtocol", dict(CachedInput=True, VerifyInput=False), "NoWrongIncarnation"),
    "metadata-no-entry-epoch-check": case("DdlProtocol", dict(Actors={"d"}, Focus="external", AllowFailover=True, Mode="no-barrier"), "NoKnownStaleExternal"),
    "metadata-two-recreations": case("DdlProtocol", expected="NeverTwoRecreations", witness=True),
    "metadata-unknown-completion": case("DdlProtocol", expected="NeverLateWrite", witness=True),
    "tree-children-first": case("DdlTree", dict(Strategy="children-first", Events=False), "PublishedHasSchema"),
    "tree-interrupted-startup": case("DdlTree", dict(Strategy="children-first", Events=False), "NoStartupFailure", omit_invariants=("PublishedHasSchema",)),
    "tree-withdraw-recovery": case("DdlTree", extra_properties=("OrphanProgress",)),
    "tree-cascade-recovery": case("DdlTree", dict(Tables={"a", "b"}), extra_properties=("OrphanProgress",)),
    "tree-retirement": case("DdlTree", dict(AllowUnknown=False, AllowFailover=False), extra_properties=("RetirementProgress",)),
    "tree-missing-assignment": case("DdlTree", dict(AllowUnknown=False, AllowFailover=False, MissingAssignment=True), extra_properties=("RetirementProgress",)),
    "tree-unknown-local": case("DdlTree", dict(AllowFailover=False)),
    "tree-queue-only-parent": case("DdlTree", dict(AllowUnknown=False, AllowFailover=False, WaitForKnown=False), "ParentWhileKnown"),
    "tree-missing-count-update": case("DdlTree", dict(AllowUnknown=False, AllowFailover=False, MaintainCounts=False), "CountsExact"),
    "tree-unknown-child-callback": case("DdlTree", dict(GuardUnknown=False), "NoEventFailure"),
    "tree-no-epoch": case("DdlTree", dict(Events=False, CheckEpoch=False), "NoOldEpochWrite"),
    "tree-unknown-residual": case("DdlTree", dict(AllowFailover=False), "NeverIncompleteWithdraw", True),
    "tree-recovered-empty": case("DdlTree", expected="NeverRecoveredWithdraw", witness=True),
}


def tla(value):
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, set):
        return "{" + ", ".join(json.dumps(item) for item in sorted(value)) + "}"
    return json.dumps(value)


def configuration(selected):
    lines = ["SPECIFICATION Spec", "CONSTANTS"]
    lines += [" " + name + " = " + tla(value)
              for name, value in selected["constants"].items()]
    lines += ["INVARIANTS " + " ".join(selected["invariants"]),
              "PROPERTIES " + " ".join(selected["properties"])]
    return "\n".join(lines) + "\n"


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_case(name, selected, source, jar, java, output, timeout):
    output.mkdir(parents=True)
    temporary = output / "tmp"
    temporary.mkdir()
    for model in source.glob("*.tla"):
        shutil.copyfile(model, output / model.name)
    (output / "model.cfg").write_text(configuration(selected))
    command = [java, "-Djava.io.tmpdir=" + str(temporary), "-cp", str(jar),
               "tlc2.TLC", "-workers", "1", "-metadir", str(output / "states"),
               "-config", "model.cfg", selected["model"] + ".tla"]
    started = time.monotonic()
    evidence = dict(case=name, command=command, expected=selected["expected"],
                    witness=selected["witness"], status="error", jar_sha256=sha256(jar),
                    cwd=str(output), config_sha256=sha256(output / "model.cfg"),
                    sources={path.name: sha256(path) for path in output.glob("*.tla")})
    try:
        with (output / "tlc.log").open("w") as log:
            result = subprocess.run(command, cwd=output, stdout=log,
                                    stderr=subprocess.STDOUT, timeout=timeout)
        evidence["exit_code"] = result.returncode
        text = (output / "tlc.log").read_text(errors="replace")
        violation = re.search(r"Error: Invariant (\S+) is violated\.", text)
        if violation and result.returncode != 0 and "State 1:" in text:
            evidence["violated_invariant"] = violation.group(1)
            if violation.group(1) == selected["expected"]:
                evidence["status"] = "expected_counterexample"
        elif (not selected["expected"] and result.returncode == 0
              and "Model checking completed. No error has been found." in text
              and "Error:" not in text):
            evidence["status"] = "bounded_pass"
        tool_version = re.search(r"^TLC2.*$", text, re.MULTILINE)
        if tool_version:
            evidence["tlc_version"] = tool_version.group(0)
        matches = re.findall(r"([\d,]+) states generated, ([\d,]+) distinct states found", text)
        if matches:
            evidence["generated"], evidence["distinct"] = [int(number.replace(",", "")) for number in matches[-1]]
    except subprocess.TimeoutExpired:
        evidence["status"] = "timeout"
    evidence["seconds"] = round(time.monotonic() - started, 3)
    (output / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jar", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--java", default="java")
    parser.add_argument("--case", action="append", choices=sorted(CASES))
    parser.add_argument("--timeout", type=int, default=90)
    args = parser.parse_args()
    jar = args.jar.resolve()
    if sha256(jar) != TLC_SHA256:
        parser.error("Use the pinned TLA+ Tools v1.7.4 jar described in README.md.")
    output = args.out.resolve()
    output.mkdir(parents=True, exist_ok=False)
    version = subprocess.run([args.java, "-version"], capture_output=True, text=True, check=True)
    source = Path(__file__).resolve().parent
    results = []
    for name in args.case or CASES:
        result = run_case(name, CASES[name], source, jar, args.java, output / name, args.timeout)
        results.append(result)
        print(name, result["status"], flush=True)
    (output / "summary.json").write_text(json.dumps(dict(java_version=version.stderr,
        runner_sha256=sha256(Path(__file__)), results=results), indent=2) + "\n")
    return 0 if all(row["status"] in {"bounded_pass", "expected_counterexample"} for row in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
