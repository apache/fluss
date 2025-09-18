#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--version) VERSION_OVERRIDE="$2"; shift 2 ;;
        -h|--help) echo "Usage: $0 [-v VERSION] [-h|--help]"; exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Find project root and get version
cd "$(dirname "$0")"
while [[ ! -f "pom.xml" && "$(pwd)" != "/" ]]; do cd ..; done
[[ ! -f "pom.xml" ]] && { echo "Error: pom.xml not found"; exit 1; }

VERSION=${VERSION_OVERRIDE:-$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)}
SHORT_VERSION=$(echo "$VERSION" | cut -d. -f1-2)
OUTPUT_DIR="website/static/javadoc/$SHORT_VERSION"

echo "Generating Javadoc $VERSION"

# Setup output
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

# Generate javadoc
export MAVEN_OPTS="--add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED"

if mvn javadoc:aggregate -Pjavadoc-aggregate -Djavadoc.aggregate=true -q 2>/dev/null; then
    [[ -d "target/site/apidocs" ]] && cp -r target/site/apidocs/* "$OUTPUT_DIR/" || { echo "No javadoc generated"; exit 1; }
else
    echo "Javadoc generation failed"; exit 1
fi

# Create redirect
mkdir -p "website/static/javadoc"
cat > "website/static/javadoc/index.html" << EOF
<!DOCTYPE html>
<html><head><meta http-equiv="refresh" content="0; url=${SHORT_VERSION}/"></head>
<body><a href="${SHORT_VERSION}/">Apache Fluss ${SHORT_VERSION} API</a></body></html>
EOF

echo "Javadoc generated: $OUTPUT_DIR"
echo "URL: http://localhost:3000/javadoc/$SHORT_VERSION/"