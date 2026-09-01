#!/usr/bin/env python3

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Generate LICENSE-bin and NOTICE-bin for the shipped Linux Gateway binary."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
from typing import Dict, Iterable, List, Set, Tuple


TARGETS = (
    "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-gnu",
)
LOCAL_PACKAGES = {"fluss-gateway", "fluss-rs"}
SEPARATOR = "-" * 80


def cargo_metadata(gateway_dir: Path, target: str) -> dict:
    command = [
        "cargo",
        "metadata",
        "--manifest-path",
        str(gateway_dir / "Cargo.toml"),
        "--locked",
        "--format-version",
        "1",
        "--filter-platform",
        target,
    ]
    return json.loads(subprocess.check_output(command, text=True))


def is_proc_macro(package: dict) -> bool:
    return any(
        "proc-macro" in target["kind"] for target in package.get("targets", ())
    )


def runtime_packages(gateway_dir: Path) -> List[dict]:
    packages: Dict[str, dict] = {}
    runtime_ids: Set[str] = set()

    for target in TARGETS:
        metadata = cargo_metadata(gateway_dir, target)
        target_packages = {
            package["id"]: package for package in metadata["packages"]
        }
        packages.update(target_packages)
        nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
        pending = [metadata["resolve"]["root"]]
        target_ids: Set[str] = set()

        while pending:
            package_id = pending.pop()
            if package_id in target_ids:
                continue
            target_ids.add(package_id)
            runtime_ids.add(package_id)
            for dependency in nodes[package_id]["deps"]:
                if (
                    any(kind["kind"] is None for kind in dependency["dep_kinds"])
                    and not is_proc_macro(target_packages[dependency["pkg"]])
                ):
                    pending.append(dependency["pkg"])

    return sorted(
        (packages[package_id] for package_id in runtime_ids),
        key=lambda package: (package["name"], package["version"]),
    )


def license_terms(expression: str) -> Tuple[str, ...]:
    """Select one compatible branch from each AND term in an SPDX expression."""

    expression = expression.replace("/", " OR ")
    selected: List[str] = []
    preferences = (
        "Apache-2.0",
        "MIT",
        "BSD-2-Clause",
        "BSD-3-Clause",
        "ISC",
        "Unicode-3.0",
        "Zlib",
        "CC0-1.0",
        "CDLA-Permissive-2.0",
    )

    for required_term in re.split(r"\s+AND\s+", expression.strip("()")):
        alternatives = [
            alternative.strip("() ")
            for alternative in re.split(r"\s+OR\s+", required_term)
        ]
        for preferred in preferences:
            if preferred in alternatives:
                selected.append(preferred)
                break
        else:
            raise RuntimeError(
                "No approved binary license selection for expression "
                f"{expression!r}."
            )

    return tuple(dict.fromkeys(selected))


def license_files(package: dict, selected_terms: Tuple[str, ...]) -> List[Path]:
    package_dir = Path(package["manifest_path"]).parent
    candidates = sorted(
        path
        for path in package_dir.rglob("*")
        if path.is_file()
        and re.match(r"(?i)^(license|copying)", path.name)
        and ".cargo-ok" not in path.name
    )

    if selected_terms == ("Apache-2.0",):
        return []

    tokens_by_license = {
        "Apache-2.0": ("apache", "boringssl"),
        "MIT": ("mit",),
        "BSD-2-Clause": ("bsd",),
        "BSD-3-Clause": ("bsd", "httprouter"),
        "ISC": ("isc", "other-bits"),
        "Unicode-3.0": ("unicode",),
        "Zlib": ("zlib",),
        "CC0-1.0": ("cc0",),
        "CDLA-Permissive-2.0": ("cdla",),
    }
    tokens = tuple(
        token
        for selected in selected_terms
        for token in tokens_by_license[selected]
    )
    matched = [
        path
        for path in candidates
        if any(token in path.name.lower() for token in tokens)
    ]

    # A package with a generically named root LICENSE file has already
    # declared which SPDX license it contains in Cargo.toml. Nested license
    # files describe code incorporated from another project and must be
    # retained even when their file name does not contain the SPDX identifier.
    generic_root_names = {"license", "license.txt", "license.md"}
    matched.extend(
        path
        for path in candidates
        if path.parent != package_dir or path.name.lower() in generic_root_names
    )
    matched = sorted(set(matched))
    matched = [
        path
        for path in matched
        if path.parent != package_dir or not is_standalone_apache_license(path)
    ]

    if not matched:
        raise RuntimeError(
            f"Cannot locate the {' AND '.join(selected_terms)} text for "
            f"{package['name']}@{package['version']}; candidates: "
            + ", ".join(str(path.relative_to(package_dir)) for path in candidates)
        )
    return matched


def is_standalone_apache_license(path: Path) -> bool:
    text = path.read_text(encoding="utf-8")
    first_line = next((line.strip() for line in text.splitlines() if line.strip()), "")
    name = path.name.lower()
    return (
        "Apache License" in first_line
        and "Version 2.0" in text
        and "TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION" in text
        and (
            "apache" in name
            or name in {"license", "license.txt", "license.md"}
        )
    )


def apache_license(repository_root: Path) -> str:
    source_license = (repository_root / "LICENSE").read_text(encoding="utf-8")
    lines = source_license.splitlines()
    separator = next(
        (
            index
            for index, line in enumerate(lines)
            if len(line) >= 20 and set(line) == {"-"}
        ),
        None,
    )
    if separator is None:
        raise RuntimeError("Cannot find the third-party section in the root LICENSE.")
    return "\n".join(lines[:separator]).rstrip() + "\n"


def gateway_notice(repository_root: Path) -> str:
    source_notice = (repository_root / "NOTICE").read_text(encoding="utf-8")
    asf_line = "The Apache Software Foundation (http://www.apache.org/)."
    asf_end = source_notice.find(asf_line)
    if asf_end < 0:
        raise RuntimeError("Cannot find the ASF attribution in the root NOTICE.")
    return source_notice[: asf_end + len(asf_line)].rstrip() + "\n"


def indent(text: str) -> str:
    lines = []
    for line in text.rstrip().splitlines():
        line = line.rstrip()
        lines.append(f"| {line}" if line else "|")
    return "\n".join(lines)


def generate_license(repository_root: Path, packages: Iterable[dict]) -> str:
    sections = [
        apache_license(repository_root),
        "",
        SEPARATOR,
        "",
        "This LICENSE covers the supported amd64 and arm64 Linux Gateway",
        "convenience binaries. Their linked dependency sets are almost identical,",
        "but a target-specific crate may appear here even when it is not linked into",
        "the other architecture's executable.",
    ]
    apache_dependencies: List[str] = []
    # Crates that ship byte-identical license text share a single reproduction, which
    # removes about a third of this file. The grouping key is the license text itself,
    # so no reproduced text is ever rewritten, reordered or dropped, and every crate
    # keeps its own attribution block.
    reproductions: Dict[Tuple[Tuple[str, str], ...], List[List[str]]] = {}

    for package in packages:
        if package["source"] is None and package["name"] in LOCAL_PACKAGES:
            continue
        expression = package.get("license")
        if not expression:
            raise RuntimeError(
                f"{package['name']}@{package['version']} has no SPDX license."
            )
        selected_terms = license_terms(expression)
        package_name = f"{package['name']}@{package['version']}"
        if "Apache-2.0" in selected_terms:
            apache_dependencies.append(package_name)

        files = license_files(package, selected_terms)
        if not files:
            continue

        package_dir = Path(package["manifest_path"]).parent
        homepage = package.get("homepage") or package.get("repository") or "(not provided)"
        attribution = [
            f"The supported Linux Gateway binaries use the Rust crate {package_name}.",
            f"Project URL: {homepage}",
            f"Declared license: {expression}",
            f"Selected license obligations: {' AND '.join(selected_terms)}",
        ]
        reproduction = tuple(
            (
                str(path.relative_to(package_dir)),
                path.read_text(encoding="utf-8").rstrip(),
            )
            for path in files
        )
        reproductions.setdefault(reproduction, []).append(attribution)

    other_sections: List[str] = []
    for reproduction, attributions in reproductions.items():
        contents = [SEPARATOR]
        for attribution in attributions:
            contents.append("")
            contents.extend(attribution)
        if len(attributions) > 1:
            contents.extend(
                [
                    "",
                    "These crates ship identical license text, reproduced once below.",
                ]
            )
        for file_name, text in reproduction:
            contents.extend(["", f"License file: {file_name}", indent(text)])
        other_sections.append("\n".join(contents))

    sections.extend(
        [
            "",
            SEPARATOR,
            "",
            "The supported Linux Gateway binaries include the following Rust crates",
            "under the Apache License, Version 2.0. Crates with additional conjunctive",
            "license obligations are also reproduced in the sections below.",
            "",
            "\n".join(apache_dependencies),
        ]
    )
    sections.extend(other_sections)
    return "\n".join(sections).rstrip() + "\n"


def generate_notice(repository_root: Path, packages: Iterable[dict]) -> str:
    unique_notices: Dict[str, Tuple[str, List[str]]] = {}

    for package in packages:
        if package["source"] is None and package["name"] in LOCAL_PACKAGES:
            continue
        package_dir = Path(package["manifest_path"]).parent
        for path in sorted(package_dir.glob("NOTICE*")):
            if not path.is_file():
                continue
            text = path.read_text(encoding="utf-8").rstrip()
            digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
            if digest not in unique_notices:
                unique_notices[digest] = (text, [])
            unique_notices[digest][1].append(
                f"{package['name']}@{package['version']}"
            )

    sections = [gateway_notice(repository_root)]
    for text, package_names in sorted(
        unique_notices.values(), key=lambda item: item[1]
    ):
        sections.extend(
            [
                "",
                SEPARATOR,
                "",
                "The following notice applies to: "
                + ", ".join(sorted(package_names)),
                "",
                text,
            ]
        )
    return "\n".join(sections).rstrip() + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail instead of updating files when generated content differs",
    )
    args = parser.parse_args()

    gateway_dir = Path(__file__).resolve().parent.parent
    repository_root = gateway_dir.parent
    packages = runtime_packages(gateway_dir)
    generated = {
        gateway_dir / "LICENSE-bin": generate_license(repository_root, packages),
        gateway_dir / "NOTICE-bin": generate_notice(repository_root, packages),
    }

    changed = []
    for path, contents in generated.items():
        current = path.read_text(encoding="utf-8") if path.exists() else None
        if current == contents:
            continue
        changed.append(path)
        if not args.check:
            path.write_text(contents, encoding="utf-8")

    if args.check and changed:
        raise SystemExit(
            "Gateway binary license files are stale: "
            + ", ".join(str(path.relative_to(repository_root)) for path in changed)
        )


if __name__ == "__main__":
    main()
