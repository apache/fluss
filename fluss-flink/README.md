<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Multi-Version Support for Flink Engine

Fluss supports multiple versions of Apache Flink by providing dedicated modules for each version.
The `fluss-flink-common` module always targets the latest version of Flink, while the `fluss-flink-${flink.version}` modules depend on both `fluss-flink-common` and the corresponding Flink version.

Occasionally, Flink's interfaces may change across versions. For example, the class `org.apache.flink.api.connector.sink2.WriterInitContext` was
introduced in Flink v1.19, while older APIs were deprecated or removed. In such cases, we address compatibility issues within the specific `fluss-flink-${flink.version}` module.
This may involve introducing placeholder classes, such as `org.apache.flink.api.connector.sink2.WriterInitContext`, to ensure successful compilation.