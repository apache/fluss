#
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
#

"""Prepare isolated, checksum-verified client classpaths from Maven Central."""

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

CENTRAL = "https://repo.maven.apache.org/maven2/"
NS = {"m": "http://maven.apache.org/POM/4.0.0"}
RUNTIME = {"slf4j-api", "lz4", "lz4-java", "snappy-java", "zstd-jni"}


def fetch(relative, destination, refresh=False):
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and not refresh:
        return
    temporary = destination.with_name(destination.name + ".download")
    try:
        subprocess.run(["curl", "--fail", "--location", "--silent", "--show-error",
                        "--connect-timeout", "10", "--max-time", "120", "--retry", "2",
                        CENTRAL + relative, "--output", str(temporary)], check=True,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)


def jar(cache, group, artifact, version):
    relative = "/".join([group.replace(".", "/"), artifact, version,
                         artifact + "-" + version + ".jar"])
    target = cache / relative
    fetch(relative, target)
    checksum = target.with_suffix(".jar.sha1")
    fetch(relative + ".sha1", checksum)
    expected = checksum.read_text().strip().split()[0].lower()
    actual = hashlib.sha1(target.read_bytes()).hexdigest()
    if actual != expected:
        raise ValueError("Maven Central checksum mismatch: " + str(target))
    return target.resolve()


def prepare(output, version):
    cache = output / "artifacts"
    relative = "org/apache/kafka/kafka-clients/" + version + "/kafka-clients-" + version + ".pom"
    pom = cache / relative
    fetch(relative, pom)
    root = ET.parse(pom).getroot()
    paths = [jar(cache, "org.apache.kafka", "kafka-clients", version)]
    dependencies = []
    test_runtime = []
    for dep in root.findall("m:dependencies/m:dependency", NS):
        scope = dep.findtext("m:scope", "compile", NS)
        group = dep.findtext("m:groupId", namespaces=NS)
        artifact = dep.findtext("m:artifactId", namespaces=NS)
        dep_version = dep.findtext("m:version", namespaces=NS)
        # Kafka 2.6.0 generated ApiVersions messages require JsonNode even for
        # byte-array producers, but its POM marks databind provided (KAFKA-10378).
        # Supply the release's exact Jackson version and its two runtime dependencies.
        if version == "2.6.0" and group == "com.fasterxml.jackson.core" and artifact == "jackson-databind":
            if scope != "provided" or dep_version != "2.10.2":
                raise ValueError("Unexpected Kafka 2.6.0 Jackson dependency: " + ET.tostring(dep).decode())
            for jackson in ("jackson-databind", "jackson-core", "jackson-annotations"):
                paths.append(jar(cache, group, jackson, dep_version))
                test_runtime.append({"coordinate": ":".join([group, jackson, dep_version]),
                                     "reason": "Kafka 2.6.0 provided Jackson closure; KAFKA-10378"})
            continue
        if scope not in ("compile", "runtime") or dep.findtext("m:optional", "false", NS) == "true":
            continue
        # These published Kafka runtime libraries are standalone JARs. Fail on a new
        # dependency rather than silently constructing an incomplete classpath.
        if artifact not in RUNTIME or not dep_version or "${" in dep_version:
            raise ValueError("Unsupported dependency; resolve its runtime closure explicitly: " + ET.tostring(dep).decode())
        paths.append(jar(cache, group, artifact, dep_version))
        dependencies.append(":".join([group, artifact, dep_version]))
        if group == "org.slf4j" and artifact == "slf4j-api":
            # Without a binding, Kafka's network-thread failures disappear into NOP logging.
            paths.append(jar(cache, group, "slf4j-simple", dep_version))
            test_runtime.append({"coordinate": ":".join([group, "slf4j-simple", dep_version]),
                                 "reason": "Isolated client diagnostics; matches published slf4j-api"})
    return paths, dependencies, test_runtime


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--versions", default="0.11.0.0,3.6.1,3.9.1,3.9.2,4.2.0,4.3.1",
                        help="Comma-separated formal releases, or 'all' for the FIP interval")
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    manifest = output / "clients.properties"
    manifest.unlink(missing_ok=True)
    metadata_path = output / "maven-metadata.xml"
    try:
        fetch("org/apache/kafka/kafka-clients/maven-metadata.xml", metadata_path, refresh=True)
    except Exception as exc:
        (output / "preparation.json").write_text(json.dumps(
            {"status": "METADATA_UNAVAILABLE_NOT_EXECUTED", "error": str(exc)}, indent=2) + "\n")
        return 1
    root = ET.parse(metadata_path).getroot()
    versions = [v.text for v in root.findall("versioning/versions/version")
                if re.fullmatch(r"[0-9]+(?:\.[0-9]+){2,3}", v.text)]
    def number(version):
        return tuple(int(x) for x in version.split("."))
    eligible = sorted([v for v in versions if number("0.11.0.0") <= number(v) <= number("4.3.1")], key=number)
    requested = eligible if args.versions == "all" else args.versions.split(",")
    report = {"source": CENTRAL + "org/apache/kafka/kafka-clients/maven-metadata.xml",
              "prepared_at": datetime.now(timezone.utc).isoformat(),
              "metadata_updated": root.findtext("versioning/lastUpdated"),
              "formal_releases_in_scope": eligible, "requested": requested,
              "not_requested": [v for v in eligible if v not in requested],
              "clients": {v: {"status": "NOT_EXECUTED"} for v in requested}}
    entries = []
    for version in requested:
        try:
            if version not in eligible:
                raise ValueError("Version is absent from the official formal-release interval")
            paths, dependencies, test_runtime = prepare(output, version)
            entries.append(version + "=" + os.pathsep.join(str(p) for p in paths))
            report["clients"][version] = {"status": "PREPARED_NOT_EXECUTED", "dependencies": dependencies,
                    "test_runtime_additions": test_runtime,
                    "jars": [{"path": str(p), "sha256": hashlib.sha256(p.read_bytes()).hexdigest()} for p in paths]}
        except Exception as exc:
            detail = getattr(exc, "stderr", b"")
            report["clients"][version] = {"status": "UNAVAILABLE_NOT_EXECUTED",
                    "error": str(exc) + " " + (detail.decode(errors="replace") if isinstance(detail, bytes) else str(detail))}
        print(version, report["clients"][version]["status"], flush=True)
        (output / "preparation.json").write_text(json.dumps(report, indent=2) + "\n")
    # Do not publish a partial executable matrix when a requested version is missing.
    manifest.unlink(missing_ok=True)
    if any(v["status"] == "UNAVAILABLE_NOT_EXECUTED" for v in report["clients"].values()):
        return 1
    manifest.write_text("\n".join(entries) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
