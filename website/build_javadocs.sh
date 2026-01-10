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

# Find project root directory
SCRIPT_PATH=$(cd "$(dirname "$0")" && pwd)
cd "$SCRIPT_PATH"

# Navigate to project root by looking for pom.xml
while [[ ! -f "pom.xml" && "$(pwd)" != "/" ]]; do
  cd ..
done

# Validate we found the project root
if [[ ! -f "pom.xml" ]]; then
  echo "Error: Could not find project root with pom.xml"
  exit 1
fi

echo "Found project root: $(pwd)"

# Extract version from Maven project
VERSION=$(./mvnw help:evaluate -Dexpression=project.version -q -DforceStdout 2>/dev/null || mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
if [[ -z "$VERSION" ]]; then
  echo "Error: Could not extract project version"
  exit 1
fi

# Get current branch name for version determination
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
echo "Current branch: $CURRENT_BRANCH"

# Determine javadoc version based on branch type
if [[ "$CURRENT_BRANCH" == "main" ]]; then
  JAVADOC_VERSION="main"
elif [[ "$CURRENT_BRANCH" =~ ^release-([0-9]+\.[0-9]+)$ ]]; then
  JAVADOC_VERSION="${BASH_REMATCH[1]}"
else
  # For feature branches or other branches, use sanitized branch name
  JAVADOC_VERSION=$(echo "$CURRENT_BRANCH" | sed 's/[^a-zA-Z0-9._-]/_/g')
fi

# Setup output directory
OUTPUT_DIR="website/static/javadoc/$JAVADOC_VERSION"
echo "Generating Javadoc for version: $JAVADOC_VERSION"
echo "Output directory: $OUTPUT_DIR"

# Create output directory
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

# Create temporary POM for targeted javadoc generation
TEMP_POM="temp-javadoc-pom.xml"
cat > "$TEMP_POM" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.apache.fluss</groupId>
    <artifactId>fluss-javadoc-temp</artifactId>
    <version>$VERSION</version>
    <packaging>pom</packaging>

    <properties>
        <maven.compiler.source>11</maven.compiler.source>
        <maven.compiler.target>11</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>

    <modules>
        <module>fluss-client</module>
        <module>fluss-flink/fluss-flink-common</module>
    </modules>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-javadoc-plugin</artifactId>
                <version>3.6.3</version>
                <configuration>
                    <windowTitle>Apache Fluss \${project.version} API</windowTitle>
                    <doctitle>Apache Fluss \${project.version} API</doctitle>
                    <doclint>none</doclint>
                    <failOnError>false</failOnError>
                    <quiet>true</quiet>
                    <additionalJOptions>
                        <additionalJOption>--add-opens=java.base/java.lang=ALL-UNNAMED</additionalJOption>
                        <additionalJOption>-J--add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED</additionalJOption>
                    </additionalJOptions>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
EOF

# Cleanup function for temporary files
cleanup() {
  if [[ -f "$TEMP_POM" ]]; then
    rm -f "$TEMP_POM"
    echo "Cleaned up temporary POM file"
  fi
}

# Set trap for cleanup on script exit
trap cleanup EXIT

# Set Maven options for Java 11+ compatibility
export MAVEN_OPTS="--add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED"

echo "Building and generating Javadoc..."

# Build and generate javadoc using temporary POM
if (./mvnw -f "$TEMP_POM" clean compile javadoc:aggregate -q 2>/dev/null || mvn -f "$TEMP_POM" clean compile javadoc:aggregate -q); then
  echo "Javadoc generation completed successfully"
else
  echo "Error: Javadoc generation failed"
  exit 1
fi

# Copy generated javadoc to output directory
if [[ -d "target/site/apidocs" ]]; then
  cp -r target/site/apidocs/* "$OUTPUT_DIR/"
  echo "Javadoc copied to: $OUTPUT_DIR"
else
  echo "Error: No javadoc files generated in target/site/apidocs"
  exit 1
fi

# Create or update redirect page
JAVADOC_INDEX="website/static/javadoc/index.html"
mkdir -p "website/static/javadoc"
cat > "$JAVADOC_INDEX" << EOF
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta http-equiv="refresh" content="0; url=${JAVADOC_VERSION}/">
    <title>Apache Fluss API Documentation</title>
</head>
<body>
    <p>Redirecting to <a href="${JAVADOC_VERSION}/">Apache Fluss ${JAVADOC_VERSION} API</a></p>
</body>
</html>
EOF

echo "Created redirect page: $JAVADOC_INDEX"

# Verify generation success
PACKAGE_COUNT=$(find "$OUTPUT_DIR" -name "package-summary.html" 2>/dev/null | wc -l | tr -d ' ')
CLASS_COUNT=$(find "$OUTPUT_DIR" -name "*.html" -not -name "index*.html" -not -name "*-summary.html" 2>/dev/null | wc -l | tr -d ' ')

echo ""
echo "Javadoc generation summary:"
echo "  Version: $JAVADOC_VERSION"
echo "  Packages: $PACKAGE_COUNT"
echo "  Classes/Interfaces: $CLASS_COUNT"
echo "  Location: $OUTPUT_DIR"
echo "  Local URL: http://localhost:3000/javadoc/$JAVADOC_VERSION/"
echo ""
echo "Javadoc generation completed successfully!"