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

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Logging functions
log_info() {
    echo "ℹ️  $1"
}

log_success() {
    echo "✅ $1"
}

log_error() {
    echo "❌ $1" >&2
}

# Utility function to copy JAR files with version numbers
copy_jar() {
    local src_pattern="$1"
    local dest_dir="$2"
    local description="$3"

    log_info "Copying $description..."

    # Find matching files
    local matches=($src_pattern)
    local count=${#matches[@]}

    # No files matched
    if (( count == 0 )); then
        log_error "No matching JAR files found: $src_pattern"
        log_error "Please build the Fluss project first: mvn clean package"
        return 1
    fi

    # Multiple files matched
    if (( count > 1 )); then
        log_error "Multiple matching JAR files found:"
        printf "    %s\n" "${matches[@]}"
        return 1
    fi

    # Exactly one file matched -> copy it with original file name
    mkdir -p "$dest_dir"
    cp "${matches[0]}" "$dest_dir/"
    log_success "Copied: $(basename "${matches[0]}")"
}

# Utility function to download and verify JAR
download_jar() {
    local url="$1"
    local dest_file="$2"
    local expected_hash="$3"
    local description="$4"

    log_info "Downloading $description..."

    # Download the file
    if ! curl -fL -o "$dest_file" "$url"; then
        log_error "Failed to download $description from $url"
        return 1
    fi

    # Verify file size
    if [ ! -s "$dest_file" ]; then
        log_error "Downloaded file is empty: $dest_file"
        return 1
    fi

    # Verify checksum if provided
    if [ -n "$expected_hash" ]; then
        local actual_hash
        actual_hash=$(shasum "$dest_file" | awk '{print $1}')
        if [ "$expected_hash" != "$actual_hash" ]; then
            log_error "Checksum mismatch for $description"
            log_error "Expected: $expected_hash"
            log_error "Actual:   $actual_hash"
            return 1
        fi
        log_success "Checksum verified for $description"
    else
        log_success "Downloaded $description"
    fi
}

# Check if required directories exist
check_prerequisites() {
    log_info "Checking prerequisites..."

    local required_dirs=(
        "$PROJECT_ROOT/fluss-flink/fluss-flink-1.20/target"
        "$PROJECT_ROOT/fluss-filesystems/fluss-fs-s3/target"
        "$PROJECT_ROOT/fluss-lake/fluss-lake-paimon/target"
        "$PROJECT_ROOT/fluss-lake/fluss-lake-iceberg/target"
        "$PROJECT_ROOT/fluss-flink/fluss-flink-tiering/target"
    )

    for dir in "${required_dirs[@]}"; do
        if [ ! -d "$dir" ]; then
            log_error "Required directory not found: $dir"
            log_error "Please build the Fluss project first: mvn clean package"
            exit 1
        fi
    done

    log_success "All prerequisites met"
}

# Main execution
main() {
    log_info "Preparing JAR files for Fluss Quickstart Flink Docker..."
    log_info "Project root: $PROJECT_ROOT"

    # Check prerequisites
    check_prerequisites

    # Clean and create directories
    log_info "Setting up directories..."
    rm -rf lib paimon iceberg opt
    mkdir -p lib paimon iceberg opt

    # Base quickstart dependencies. These are always copied into /opt/flink/lib
    # so the regular Flink quickstart works without any extra setup.
    log_info "Preparing base quickstart dependencies..."
    copy_jar "$PROJECT_ROOT/fluss-flink/fluss-flink-1.20/target/fluss-flink-1.20-*.jar" "./lib" "fluss-flink-1.20 connector"
    copy_jar "$PROJECT_ROOT/fluss-filesystems/fluss-fs-s3/target/fluss-fs-s3-*.jar" "./lib" "fluss-fs-s3 filesystem plugin"

    # Shared helper used by the Flink quickstart SQL demo.
    download_jar \
        "https://github.com/knaufk/flink-faker/releases/download/v0.5.3/flink-faker-0.5.3.jar" \
        "./lib/flink-faker-0.5.3.jar" \
        "" \
        "flink-faker-0.5.3"

    # Paimon-specific dependencies. These stay outside /opt/flink/lib by
    # default and are activated by init_paimon.sh only when needed.
    log_info "Preparing optional Paimon lakehouse dependencies..."
    copy_jar "$PROJECT_ROOT/fluss-lake/fluss-lake-paimon/target/fluss-lake-paimon-*.jar" "./paimon" "fluss-lake-paimon connector"
    download_jar \
        "https://repo1.maven.org/maven2/io/trino/hadoop/hadoop-apache/3.3.5-2/hadoop-apache-3.3.5-2.jar" \
        "./paimon/hadoop-apache-3.3.5-2.jar" \
        "508255883b984483a45ca48d5af6365d4f013bb8" \
        "hadoop-apache-3.3.5-2"
    download_jar \
        "https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-1.20/1.3.1/paimon-flink-1.20-1.3.1.jar" \
        "./paimon/paimon-flink-1.20-1.3.1.jar" \
        "" \
        "paimon-flink-1.20-1.3.1"
    download_jar \
        "https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-s3/1.3.1/paimon-s3-1.3.1.jar" \
        "./paimon/paimon-s3-1.3.1.jar" \
        "" \
        "paimon-s3-1.3.1"

    # Iceberg-specific dependencies. These stay outside /opt/flink/lib by
    # default and are activated by init_iceberg.sh only when needed.
    log_info "Preparing optional Iceberg lakehouse dependencies..."
    copy_jar "$PROJECT_ROOT/fluss-lake/fluss-lake-iceberg/target/fluss-lake-iceberg-*.jar" "./iceberg" "fluss-lake-iceberg connector"
    download_jar \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/1.10.1/iceberg-flink-runtime-1.20-1.10.1.jar" \
        "./iceberg/iceberg-flink-runtime-1.20-1.10.1.jar" \
        "" \
        "iceberg-flink-runtime-1.20-1.10.1"
    download_jar \
        "https://repo1.maven.org/maven2/io/trino/hadoop/hadoop-apache/3.3.5-2/hadoop-apache-3.3.5-2.jar" \
        "./iceberg/hadoop-apache-3.3.5-2.jar" \
        "508255883b984483a45ca48d5af6365d4f013bb8" \
        "hadoop-apache-3.3.5-2"
    download_jar \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/1.10.1/iceberg-aws-1.10.1.jar" \
        "./iceberg/iceberg-aws-1.10.1.jar" \
        "" \
        "iceberg-aws-1.10.1"
    download_jar \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar" \
        "./iceberg/iceberg-aws-bundle-1.10.1.jar" \
        "" \
        "iceberg-aws-bundle-1.10.1"
    download_jar \
        "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar" \
        "./iceberg/postgresql-42.7.4.jar" \
        "" \
        "postgresql-42.7.4"

    # Prepare lake tiering JAR
    log_info "Preparing lake tiering JAR..."
    copy_jar "$PROJECT_ROOT/fluss-flink/fluss-flink-tiering/target/fluss-flink-tiering-*.jar" "./opt" "fluss-flink-tiering"

    # Final verification
    verify_jars

    # Show summary
    show_summary
}

# Verify that all required JAR files are present
verify_jars() {
    log_info "Verifying all required JAR files are present..."

    local missing_jars=()
    local lib_jars=(
        "fluss-flink-1.20-*.jar"
        "fluss-fs-s3-*.jar"
        "flink-faker-0.5.3.jar"
    )

    local paimon_jars=(
        "fluss-lake-paimon-*.jar"
        "hadoop-apache-3.3.5-2.jar"
        "paimon-flink-1.20-1.3.1.jar"
        "paimon-s3-1.3.1.jar"
    )

    local iceberg_jars=(
        "fluss-lake-iceberg-*.jar"
        "iceberg-flink-runtime-1.20-1.10.1.jar"
        "hadoop-apache-3.3.5-2.jar"
        "iceberg-aws-1.10.1.jar"
        "iceberg-aws-bundle-1.10.1.jar"
        "postgresql-42.7.4.jar"
    )

    local opt_jars=(
        "fluss-flink-tiering-*.jar"
    )

    # Check lib directory
    for jar_pattern in "${lib_jars[@]}"; do
        if ! ls ./lib/$jar_pattern >/dev/null 2>&1; then
            missing_jars+=("lib/$jar_pattern")
        fi
    done

    for jar_pattern in "${paimon_jars[@]}"; do
        if ! ls ./paimon/$jar_pattern >/dev/null 2>&1; then
            missing_jars+=("paimon/$jar_pattern")
        fi
    done

    for jar_pattern in "${iceberg_jars[@]}"; do
        if ! ls ./iceberg/$jar_pattern >/dev/null 2>&1; then
            missing_jars+=("iceberg/$jar_pattern")
        fi
    done

    # Check opt directory
    for jar_pattern in "${opt_jars[@]}"; do
        if ! ls ./opt/$jar_pattern >/dev/null 2>&1; then
            missing_jars+=("opt/$jar_pattern")
        fi
    done

    # Report results
    if [ ${#missing_jars[@]} -eq 0 ]; then
        log_success "All required JAR files are present!"
    else
        log_error "Missing required JAR files:"
        for jar in "${missing_jars[@]}"; do
            log_error "  - $jar"
        done
        exit 1
    fi
}

# Summary function
show_summary() {
    log_success "JAR files preparation completed!"
    echo ""
    log_info "Generated JAR files:"
    echo ""
    echo "lib/ (base quickstart dependencies copied into /opt/flink/lib):"
    ls -lh ./lib/ | tail -n +2 | awk '{printf "  %-50s %8s\n", $9, $5}'
    echo ""
    echo "paimon/ (optional dependencies activated by init_paimon.sh):"
    ls -lh ./paimon/ | tail -n +2 | awk '{printf "  %-50s %8s\n", $9, $5}'
    echo ""
    echo "iceberg/ (optional dependencies activated by init_iceberg.sh):"
    ls -lh ./iceberg/ | tail -n +2 | awk '{printf "  %-50s %8s\n", $9, $5}'
    echo ""
    echo "opt/ (tiering service):"
    ls -lh ./opt/ | tail -n +2 | awk '{printf "  %-50s %8s\n", $9, $5}'
    echo ""
    log_info "Included Components:"
    echo "  - Base: Fluss Flink 1.20 connector"
    echo "  - Base: Fluss S3 filesystem plugin"
    echo "  - Base: Flink Faker (v0.5.3)"
    echo "  - Paimon only: Fluss Lake Paimon connector"
    echo "  - Paimon only: Paimon Flink 1.20 (v1.3.1)"
    echo "  - Paimon only: Paimon S3 (v1.3.1)"
    echo "  - Paimon only: Hadoop Apache (v3.3.5-2)"
    echo "  - Iceberg only: Fluss Lake Iceberg connector"
    echo "  - Iceberg only: Iceberg Flink runtime 1.20 (v1.10.1)"
    echo "  - Iceberg only: Hadoop Apache (v3.3.5-2)"
    echo "  - Iceberg only: Iceberg AWS (v1.10.1)"
    echo "  - Iceberg only: Iceberg AWS bundle (v1.10.1)"
    echo "  - Iceberg only: PostgreSQL JDBC (v42.7.4)"
    echo "  - Shared opt/: Fluss Tiering service"
}

# Run main function
main "$@"
