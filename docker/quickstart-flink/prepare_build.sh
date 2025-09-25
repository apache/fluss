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

echo "Preparing jar files for Fluss Quickstart Flink Docker..."

# Get the project root directory (assuming this script is in docker/quickstart-flink/)
PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# Clean and create lib directory for Flink connector JARs
echo "Creating lib directory for Flink connector JARs..."
rm -rf lib
mkdir lib

# Copy Fluss connector JARs (these need to be built first)
echo "Copying Fluss connector JARs..."
cp "$PROJECT_ROOT/fluss-flink/fluss-flink-1.20/target/fluss-flink-1.20-*.jar" ./lib/fluss-flink-1.20.jar
cp "$PROJECT_ROOT/fluss-lake/fluss-lake-paimon/target/fluss-lake-paimon-*.jar" ./lib/fluss-lake-paimon.jar

# Download external dependencies
echo "Downloading external dependencies..."

# Download flink-faker for data generation
wget -O ./lib/flink-faker-0.5.3.jar "https://github.com/knaufk/flink-faker/releases/download/v0.5.3/flink-faker-0.5.3.jar"

# Download flink-shaded-hadoop-2-uber for Hadoop integration
wget -O ./lib/flink-shaded-hadoop-2-uber.jar "https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar"

# Download paimon-flink connector (commented out as it might not be needed)
wget -O ./lib/paimon-flink-1.20-1.2.0.jar "https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-1.20/1.2.0/paimon-flink-1.20-1.2.0.jar"

# Prepare lake tiering JAR and put it into opt/ directory
echo "Preparing lake tiering JAR..."
rm -rf opt
mkdir opt
cp "$PROJECT_ROOT/fluss-flink/fluss-flink-tiering/target/fluss-flink-tiering-*.jar" ./opt/fluss-flink-tiering.jar

echo "Jar files preparation completed!"
