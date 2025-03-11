---
sidebar_position: 3
---

<!--
 Copyright (c) 2025 Alibaba Group Holding Ltd.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# TTL

Fluss supports TTL for data by setting the TTL attribute for tables with `'table.log.ttl' = '<duration>'` (default is 7 days). Fluss can periodically and automatically check for and clean up expired data in the table.

For log tables, this attribute indicates the expiration time of the log table data.
For primary key tables, this attribute indicates the expiration time of their binlog and does not represent the expiration time of the primary key table data. If you also want the data in the primary key table to expire automatically, please use [auto partitioning](partitioning.md).
