#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

    # Exactly one file matched → copy it with original file name
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
    if ! wget -O "$dest_file" "$url"; then
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
        local actual_hash=$(shasum "$dest_file" | awk '{print $1}')
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
        "$PROJECT_ROOT/fluss-lake/fluss-lake-paimon/target"
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
    rm -rf lib opt
    mkdir -p lib opt

    # Copy Fluss connector JARs
    log_info "Copying Fluss connector JARs..."
    copy_jar "$PROJECT_ROOT/fluss-flink/fluss-flink-1.20/target/fluss-flink-1.20-*.jar" "./lib" "fluss-flink-1.20 connector"
    copy_jar "$PROJECT_ROOT/fluss-lake/fluss-lake-paimon/target/fluss-lake-paimon-*.jar" "./lib" "fluss-lake-paimon connector"

    # Download external dependencies
    log_info "Downloading external dependencies..."

    # Download flink-faker for data generation
    download_jar \
        "https://github.com/knaufk/flink-faker/releases/download/v0.5.3/flink-faker-0.5.3.jar" \
        "./lib/flink-faker-0.5.3.jar" \
        "" \
        "flink-faker-0.5.3"

    # Download flink-shaded-hadoop-2-uber for Hadoop integration
    download_jar \
        "https://repo1.maven.org/maven2/io/trino/hadoop/hadoop-apache/3.3.5-2/hadoop-apache-3.3.5-2.jar" \
        "./lib/hadoop-apache-3.3.5-2.jar" \
        "508255883b984483a45ca48d5af6365d4f013bb8" \
        "hadoop-apache-3.3.5-2.jar"

    # Download paimon-flink connector
    download_jar \
        "https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-1.20/1.2.0/paimon-flink-1.20-1.2.0.jar" \
        "./lib/paimon-flink-1.20-1.2.0.jar" \
        "b9f8762c6e575f6786f1d156a18d51682ffc975c" \
        "paimon-flink-1.20-1.2.0"

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
        "fluss-lake-paimon-*.jar"
        "flink-faker-0.5.3.jar"
        "hadoop-apache-3.3.5-2.jar"
        "paimon-flink-1.20-1.2.0.jar"
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
    log_info "📦 Generated JAR files:"
    echo "Lib directory:"
    ls -la ./lib/ 2>/dev/null || echo "  (empty)"
    echo "Opt directory:"
    ls -la ./opt/ 2>/dev/null || echo "  (empty)"
}

# Run main function
main "$@"
