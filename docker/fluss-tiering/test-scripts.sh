#!/usr/bin/env bash

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

set -o errexit
set -o nounset
set -o pipefail

# Exercises the shell contracts without Java, Docker, downloads, or Maven.
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
REPOSITORY_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd -P)"
TEST_ROOT="$(mktemp -d)"
trap 'rm -rf "${TEST_ROOT}"' EXIT
mkdir "${TEST_ROOT}/bin"

# Only the fixed container staging path changes in this isolated script copy.
sed "s|^RUNTIME_CONFIG_FILE=.*|RUNTIME_CONFIG_FILE=\"${TEST_ROOT}/runtime/tiering.yaml\"|" \
    "${SCRIPT_DIR}/docker-entrypoint.sh" > "${TEST_ROOT}/docker-entrypoint.sh"

cat > "${TEST_ROOT}/bin/java" <<'EOF'
#!/usr/bin/env bash
set -eu
printf '%s\n' "$@" > "${CAPTURE_DIR}/java-args"
printf '%s\n' "${JAVA_TOOL_OPTIONS}" > "${CAPTURE_DIR}/java-options"
while [[ "$#" -gt 0 ]]; do
    if [[ "$1" == "--config.file" ]]; then
        cp "$2" "${CAPTURE_DIR}/staged.yaml"
        exit 0
    fi
    shift
done
exit 1
EOF

cat > "${TEST_ROOT}/bin/curl" <<'EOF'
#!/usr/bin/env bash
echo 'Unexpected network access in shell regression test.' >&2
exit 1
EOF

# This Docker fixture models the validation error even with an unresolved
# bootstrap value. The smoke test must inspect the staged file, not that error.
cat > "${TEST_ROOT}/bin/docker" <<'EOF'
#!/usr/bin/env bash
set -eu
case "$1" in
    rm) exit 0 ;;
    inspect) echo 1 ;;
    cp)
        case "${STAGED_VALUE}" in
            resolved|launch-error)
                printf '%s\n' 'fluss.bootstrap.servers: smoke-bootstrap:9123' > "$3" ;;
            unresolved)
                printf '%s\n' 'fluss.bootstrap.servers: ${BOOTSTRAP_HOST}' > "$3" ;;
            omitted)
                printf '%s\n' '# No bootstrap entry' > "$3" ;;
            *) exit 1 ;;
        esac
        ;;
    run)
        shift
        substitution=false
        while [[ "$1" != "regression-image" ]]; do
            if [[ "$1" == FLUSS_TIERING_PROPERTIES=* ]]; then
                substitution=true
            fi
            shift
        done
        shift
        case "${1:-}" in
            bash)
                printf '%s\n' fluss-flink-tiering-fixture.jar fluss-flink-1.20-fixture.jar \
                    fluss-lake-paimon-fixture.jar fluss-lake-iceberg-fixture.jar \
                    fluss-lake-lance-fixture.jar paimon-bundle-fixture.jar \
                    iceberg-flink-runtime-1.20-fixture.jar flink-shaded-hadoop-2-uber-fixture.jar
                ;;
            help) echo 'Usage: tieringService' ;;
            '')
                if [[ "${substitution}" == true ]]; then
                    if [[ "${STAGED_VALUE}" == launch-error ]]; then
                        echo 'Service launch failed.'
                    else
                        echo 'datalake.format is not configured'
                    fi
                else
                    echo 'The bootstrap server to fluss is not configured'
                fi
                exit 1
                ;;
            *) exit 1 ;;
        esac
        ;;
    *) exit 1 ;;
esac
EOF
chmod +x "${TEST_ROOT}/bin/"*
# Exercise the scripts and command fixtures under the invoking Bash version.
ln -s "${BASH}" "${TEST_ROOT}/bin/bash"

assert_line() {
    if ! grep -Fxq -- "$1" "$2"; then
        printf 'Missing exact line: %s\nFile: %s\n' "$1" "$2" >&2
        cat "$2" >&2
        return 1
    fi
}

run_entrypoint() {
    env -i PATH="${TEST_ROOT}/bin:${PATH}" HOME="${TEST_ROOT}" \
        FLUSS_HOME="${CASE_DIR}" FLINK_HOME="${CASE_DIR}/flink" \
        TIERING_CONFIG_FILE="${CASE_DIR}/input.yaml" CAPTURE_DIR="${CASE_DIR}" \
        "$@" bash "${TEST_ROOT}/docker-entrypoint.sh" tieringService --test-argument
}

test_shipped_template() {
    cp "${SCRIPT_DIR}/tiering.yaml" "${CASE_DIR}/input.yaml"
    run_entrypoint BOOTSTRAP_HOST=template-bootstrap:9123 \
        FLUSS_TIERING_PROPERTIES=$'fluss.bootstrap.servers: ${BOOTSTRAP_HOST}\ndatalake.format: paimon'
    assert_line 'fluss.bootstrap.servers: template-bootstrap:9123' "${CASE_DIR}/staged.yaml"
    assert_line 'datalake.format: paimon' "${CASE_DIR}/staged.yaml"
    cmp "${SCRIPT_DIR}/tiering.yaml" "${CASE_DIR}/input.yaml"
    assert_line '--test-argument' "${CASE_DIR}/java-args"
    assert_line '--add-opens=java.base/java.nio=ALL-UNNAMED' "${CASE_DIR}/java-args"
    assert_line '-XX:MaxRAMPercentage=60.0 -XX:MaxDirectMemorySize=512m' "${CASE_DIR}/java-options"
}

test_comments_and_provider_references() {
    cat > "${CASE_DIR}/input.yaml" <<'EOF'
# ${FULL_LINE_COMMENT} ${BOOTSTRAP_HOST}
fluss.bootstrap.servers: ${BOOTSTRAP_HOST} # ${INLINE_COMMENT} ${BOOTSTRAP_HOST}
datalake.format: paimon
quoted: "value#${QUOTED_COMMENT} ${BOOTSTRAP_HOST}"
password: $SECRET
provider: ${provider:env:BOOTSTRAP_HOST}
EOF
    run_entrypoint BOOTSTRAP_HOST=inline-bootstrap:9123 SECRET=must-not-expand \
        JAVA_TOOL_OPTIONS=-Xmx128m
    assert_line '# ${FULL_LINE_COMMENT} ${BOOTSTRAP_HOST}' "${CASE_DIR}/staged.yaml"
    assert_line 'fluss.bootstrap.servers: inline-bootstrap:9123 # ${INLINE_COMMENT} ${BOOTSTRAP_HOST}' "${CASE_DIR}/staged.yaml"
    assert_line 'quoted: "value#${QUOTED_COMMENT} ${BOOTSTRAP_HOST}"' "${CASE_DIR}/staged.yaml"
    assert_line 'password: $SECRET' "${CASE_DIR}/staged.yaml"
    assert_line 'provider: ${provider:env:BOOTSTRAP_HOST}' "${CASE_DIR}/staged.yaml"
    assert_line '-Xmx128m' "${CASE_DIR}/java-options"
}

test_bare_references_stay_literal() {
    cat > "${CASE_DIR}/input.yaml" <<'EOF'
username: ${USER}
password: literal$USER
mixed: $USER/${USER}/$USER/${USER}${USER}/$USER_SUFFIX
other: ${USER_SUFFIX}/${_TOKEN_2}/$USER_SUFFIX/$_TOKEN_2
unsupported: ${USER:-fallback} ${1} $1 $$USER $9USER ${USER.NAME}
EOF
    run_entrypoint USER=alice USER_SUFFIX=bob _TOKEN_2=carol
    cat > "${CASE_DIR}/expected.yaml" <<'EOF'
username: alice
password: literal$USER
mixed: $USER/alice/$USER/alicealice/$USER_SUFFIX
other: bob/carol/$USER_SUFFIX/$_TOKEN_2
unsupported: ${USER:-fallback} ${1} $1 $$USER $9USER ${USER.NAME}
EOF
    diff -u "${CASE_DIR}/expected.yaml" "${CASE_DIR}/staged.yaml"
}

test_appended_bare_references_stay_literal() {
    printf '%s\n' 'datalake.format: paimon' > "${CASE_DIR}/input.yaml"
    run_entrypoint USER=alice \
        FLUSS_TIERING_PROPERTIES=$'username: ${USER}\npassword: literal$USER'
    assert_line 'username: alice' "${CASE_DIR}/staged.yaml"
    assert_line 'password: literal$USER' "${CASE_DIR}/staged.yaml"
}

test_replacement_values_are_literal() {
    cat > "${CASE_DIR}/input.yaml" <<'EOF'
username: ${USER}
password: <${PASSWORD}>
EOF
    local password='literal$USER ${USER} ${UNSET} & \\ \n \t \1
# multiline $(touch "${CAPTURE_DIR}/executed") `touch "${CAPTURE_DIR}/executed"`
end\

'
    run_entrypoint USER=alice PASSWORD="${password}"
    cat > "${CASE_DIR}/expected.yaml" <<'EOF'
username: alice
password: <literal$USER ${USER} ${UNSET} & \\ \n \t \1
# multiline $(touch "${CAPTURE_DIR}/executed") `touch "${CAPTURE_DIR}/executed"`
end\

>
EOF
    diff -u "${CASE_DIR}/expected.yaml" "${CASE_DIR}/staged.yaml"
    [[ ! -e "${CASE_DIR}/executed" ]]
}

test_no_braced_references() {
    cat > "${CASE_DIR}/input.yaml" <<'EOF'
# ${MISSING} ${USER}
password: $USER
provider: ${provider:env:USER}
EOF
    run_entrypoint USER=alice
    cmp "${CASE_DIR}/input.yaml" "${CASE_DIR}/staged.yaml"
}

test_missing_and_empty_variables() {
    printf '%s\n' 'fluss.bootstrap.servers: ${BOOTSTRAP_HOST}' > "${CASE_DIR}/input.yaml"
    if run_entrypoint > "${CASE_DIR}/missing.log" 2>&1; then
        echo 'An unset referenced variable must fail.' >&2
        return 1
    fi
    if run_entrypoint BOOTSTRAP_HOST= > "${CASE_DIR}/empty.log" 2>&1; then
        echo 'An empty referenced variable must fail.' >&2
        return 1
    fi
    local log
    for log in missing empty; do
        assert_line 'The tiering configuration references the environment variable BOOTSTRAP_HOST, which is not set.' \
            "${CASE_DIR}/${log}.log"
    done
    [[ ! -e "${CASE_DIR}/java-args" ]]
}

test_append_without_final_newline() {
    printf 'datalake.format: paimon' > "${CASE_DIR}/input.yaml"
    run_entrypoint FLUSS_TIERING_PROPERTIES='fluss.bootstrap.servers: appended-bootstrap:9123'
    assert_line 'datalake.format: paimon' "${CASE_DIR}/staged.yaml"
    assert_line 'fluss.bootstrap.servers: appended-bootstrap:9123' "${CASE_DIR}/staged.yaml"
    printf 'datalake.format: paimon' > "${CASE_DIR}/expected-source.yaml"
    cmp "${CASE_DIR}/expected-source.yaml" "${CASE_DIR}/input.yaml"
}

test_entrypoint_modes() {
    env -i PATH="${TEST_ROOT}/bin:${PATH}" FLUSS_HOME="${CASE_DIR}" \
        bash "${TEST_ROOT}/docker-entrypoint.sh" help > "${CASE_DIR}/help.log"
    grep -q tieringService "${CASE_DIR}/help.log"
    env -i PATH="${TEST_ROOT}/bin:${PATH}" FLUSS_HOME="${CASE_DIR}" \
        bash "${TEST_ROOT}/docker-entrypoint.sh" printf '%s\n' pass-through > "${CASE_DIR}/command.log"
    assert_line pass-through "${CASE_DIR}/command.log"
}

paimon_version="$(sed -n 's/.*<paimon.version>\(.*\)<\/paimon.version>.*/\1/p' "${REPOSITORY_ROOT}/pom.xml")"
iceberg_version="$(sed -n 's/.*<iceberg.version>\(.*\)<\/iceberg.version>.*/\1/p' "${REPOSITORY_ROOT}/pom.xml")"
hadoop_version="$(grep -A1 '<artifactId>flink-shaded-hadoop-2-uber</artifactId>' "${REPOSITORY_ROOT}/fluss-dist/pom.xml" | sed -n 's/.*<version>\(.*\)<\/version>.*/\1/p')"
fluss_artifacts=(fluss-flink-tiering fluss-flink-1.20 fluss-lake-paimon fluss-lake-iceberg fluss-lake-lance)

create_jar() {
    local directory="${CASE_DIR}/m2/$1/$2/$3"
    mkdir -p "${directory}"
    printf 'fixture %s\n' "$3" > "${directory}/$2-$3.jar"
}

create_m2() {
    local version artifact
    for version in round1-a round1-b; do
        for artifact in "${fluss_artifacts[@]}"; do
            create_jar org/apache/fluss "${artifact}" "${version}"
        done
    done
    create_jar org/apache/paimon paimon-bundle "${paimon_version}"
    create_jar org/apache/iceberg iceberg-flink-runtime-1.20 "${iceberg_version}"
    create_jar org/apache/flink flink-shaded-hadoop-2-uber "${hadoop_version}"
    mkdir "${CASE_DIR}/output"
    echo 'Keep sibling build inputs.' > "${CASE_DIR}/output/keep.txt"
}

run_prepare() {
    env PATH="${CASE_DIR}/bin:${TEST_ROOT}/bin:${PATH}" MAVEN_LOCAL_REPO="${CASE_DIR}/m2" \
        TIERING_IMAGE_BUILD_DIR="${CASE_DIR}/output" TIERING_IMAGE_RELEASE_VERSION="$1" \
        SKIP_DOWNLOAD=true bash "${SCRIPT_DIR}/prepare_build.sh"
}

assert_lib_version() {
    local artifact jar
    {
        for artifact in "${fluss_artifacts[@]}"; do
            echo "${artifact}-$1.jar"
        done
        echo "paimon-bundle-${paimon_version}.jar"
        echo "iceberg-flink-runtime-1.20-${iceberg_version}.jar"
        echo "flink-shaded-hadoop-2-uber-${hadoop_version}.jar"
    } | sort > "${CASE_DIR}/expected-jars"
    for jar in "${CASE_DIR}/output/lib/"*; do
        basename "${jar}"
    done | sort > "${CASE_DIR}/actual-jars"
    diff -u "${CASE_DIR}/expected-jars" "${CASE_DIR}/actual-jars"
    assert_line 'Keep sibling build inputs.' "${CASE_DIR}/output/keep.txt"
    shopt -s nullglob dotglob
    local entries=("${CASE_DIR}/output/"*)
    [[ "${#entries[@]}" -eq 2 ]]
}

test_prepare_refreshes_lib() {
    create_m2
    run_prepare round1-a
    run_prepare round1-b
    run_prepare round1-b
    assert_lib_version round1-b
}

test_failed_prepare_preserves_lib() {
    create_m2
    run_prepare round1-a
    # The final dependency is absent, after all Fluss jars can be staged.
    rm "${CASE_DIR}/m2/org/apache/flink/flink-shaded-hadoop-2-uber/${hadoop_version}/flink-shaded-hadoop-2-uber-${hadoop_version}.jar"
    if run_prepare round1-b > "${CASE_DIR}/failure.log" 2>&1; then
        echo 'Preparation must fail when a required jar is absent.' >&2
        return 1
    fi
    grep -q 'Missing jar (SKIP_DOWNLOAD is set)' "${CASE_DIR}/failure.log"
    assert_lib_version round1-a
    local artifact
    for artifact in "${fluss_artifacts[@]}"; do
        assert_line 'fixture round1-a' "${CASE_DIR}/output/lib/${artifact}-round1-a.jar"
    done
}

test_failed_publication_restores_lib() {
    create_m2
    run_prepare round1-a
    mkdir "${CASE_DIR}/bin"
    cat > "${CASE_DIR}/bin/mv" <<'EOF'
#!/usr/bin/env bash
set -eu
if [[ "$1" == */.lib.*/lib ]]; then
    echo 'Simulated lib publication failure.' >&2
    exit 1
fi
exec /bin/mv "$@"
EOF
    chmod +x "${CASE_DIR}/bin/mv"
    if run_prepare round1-b > "${CASE_DIR}/failure.log" 2>&1; then
        echo 'Preparation must fail when lib publication fails.' >&2
        return 1
    fi
    assert_line 'Simulated lib publication failure.' "${CASE_DIR}/failure.log"
    assert_lib_version round1-a
}

test_prepare_does_not_follow_lib_symlink() {
    create_m2
    mkdir "${CASE_DIR}/external-lib"
    echo 'Keep the symlink target.' > "${CASE_DIR}/external-lib/keep.txt"
    ln -s "${CASE_DIR}/external-lib" "${CASE_DIR}/output/lib"
    run_prepare round1-b
    assert_lib_version round1-b
    [[ ! -L "${CASE_DIR}/output/lib" ]]
    assert_line 'Keep the symlink target.' "${CASE_DIR}/external-lib/keep.txt"
    local entries=("${CASE_DIR}/external-lib/"*)
    [[ "${#entries[@]}" -eq 1 ]]
}

run_smoke() {
    env -i PATH="${TEST_ROOT}/bin:${PATH}" HOME="${TEST_ROOT}" TMPDIR="${CASE_DIR}" \
        TIERING_IMAGE=regression-image STAGED_VALUE="$1" bash "${SCRIPT_DIR}/smoke-test.sh"
}

test_smoke_accepts_substituted_value() {
    run_smoke resolved
}

assert_smoke_rejects() {
    if run_smoke "$1" > "${CASE_DIR}/smoke.log" 2>&1; then
        echo "Smoke substitution assertion accepted $1 staged value." >&2
        return 1
    fi
    assert_line 'The entrypoint did not substitute the FLUSS_TIERING_PROPERTIES ${VAR} reference.' "${CASE_DIR}/smoke.log"
    if grep -q 'unbound variable' "${CASE_DIR}/smoke.log"; then
        cat "${CASE_DIR}/smoke.log" >&2
        return 1
    fi
}

test_smoke_rejects_unresolved_value() {
    assert_smoke_rejects unresolved
}

test_smoke_rejects_omitted_value() {
    assert_smoke_rejects omitted
}

test_smoke_literal_diagnostic() {
    assert_smoke_rejects launch-error
}

passed=0
failed=0
for test in test_shipped_template test_comments_and_provider_references \
    test_bare_references_stay_literal test_appended_bare_references_stay_literal \
    test_replacement_values_are_literal test_no_braced_references \
    test_missing_and_empty_variables test_append_without_final_newline test_entrypoint_modes \
    test_prepare_refreshes_lib test_failed_prepare_preserves_lib \
    test_failed_publication_restores_lib test_prepare_does_not_follow_lib_symlink \
    test_smoke_accepts_substituted_value test_smoke_rejects_unresolved_value \
    test_smoke_rejects_omitted_value test_smoke_literal_diagnostic; do
    CASE_DIR="${TEST_ROOT}/${test}"
    mkdir "${CASE_DIR}"
    # Run outside a conditional so errexit remains active inside each test.
    set +e
    (set -e; "${test}") > "${CASE_DIR}/test.log" 2>&1
    status=$?
    set -e
    if [[ "${status}" -eq 0 ]]; then
        echo "PASS ${test}"
        passed=$((passed + 1))
    else
        echo "FAIL ${test}"
        cat "${CASE_DIR}/test.log"
        failed=$((failed + 1))
    fi
done
printf '%s passed, %s failed\n' "${passed}" "${failed}"
[[ "${failed}" -eq 0 ]]
