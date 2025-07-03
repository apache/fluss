---
title: "Merge Engines"
sidebar_position: 1
---

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

# Merge Engines

The **Merge Engine** in Fluss is a core component designed to efficiently handle and consolidate data updates for PrimaryKey Tables.
It offers users the flexibility to define how incoming data records are merged with existing records sharing the same primary key.
However, users can specify a different merge engine to customize the merging behavior according to their specific use cases

The following merge engines are supported:

1. [Default Merge Engine (LastRow)](table-design/table-types/pk-table/merge-engines/default.md)
2. [FirstRow Merge Engine](table-design/table-types/pk-table/merge-engines/first-row.md)
3. [Versioned Merge Engine](table-design/table-types/pk-table/merge-engines/versioned.md)