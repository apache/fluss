#
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

FROM flink:1.20-java17

ARG FLUSS_VERSION=0.7.0
ARG PAIMON_VERSION=1.2.0
ARG FLINK_VERSION=1.20
ARG HADOOP_VERSION=2.8.3-10.0

RUN wget -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-${FLINK_VERSION}/${PAIMON_VERSION}/paimon-flink-${FLINK_VERSION}-${PAIMON_VERSION}.jar
RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/${HADOOP_VERSION}/flink-shaded-hadoop-2-uber-${HADOOP_VERSION}.jar
RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/com/alibaba/fluss/fluss-flink-${FLINK_VERSION}/${FLUSS_VERSION}/fluss-flink-${FLINK_VERSION}-${FLUSS_VERSION}.jar
RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/com/alibaba/fluss/fluss-lake-paimon/${FLUSS_VERSION}/fluss-lake-paimon-${FLUSS_VERSION}.jar
RUN wget -P /opt/flink/opt \
    https://repo1.maven.org/maven2/com/alibaba/fluss/fluss-flink-tiering/${FLUSS_VERSION}/fluss-flink-tiering-${FLUSS_VERSION}.jar
RUN wget -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-s3/${PAIMON_VERSION}/paimon-s3-${PAIMON_VERSION}.jar

