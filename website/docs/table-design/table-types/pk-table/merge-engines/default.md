---
sidebar_label: Default (LastRow)
title: Default Merge Engine
sidebar_position: 2
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

# Default Merge Engine (LastRow)

## Overview

The **Default Merge Engine** behaves as a LastRow merge engine that retains the latest record for a given primary key. It supports all the operations: `INSERT`, `UPDATE`, `DELETE`.
Additionally, the default merge engine supports [Partial Update](table-design/table-types/pk-table/index.md#partial-update), which preserves the latest values for the specified update columns.
If the `'table.merge-engine'` property is not explicitly defined in the table properties when creating a Primary Key Table, the default merge engine will be applied automatically.


## Example

```sql title="Flink SQL"
CREATE TABLE T (
    k  INT,
    v1 DOUBLE,
    v2 STRING,
    PRIMARY KEY (k) NOT ENFORCED
);

-- Insert
INSERT INTO T(k, v1, v2) VALUES (1, 1.0, 't1');
INSERT INTO T(k, v1, v2) VALUES (1, 1.0, 't2');
SELECT * FROM T WHERE k = 1;
-- Output:
+----+-----+----+
| k  | v1  | v2 |
+----+-----+----+
| 1  | 1.0 | t2 |
+----+-----+----+

-- Update
INSERT INTO T(k, v1, v2) VALUES (2, 2.0, 't2');
-- Switch to batch mode to perform update operation for UPDATE statement is only supported for batch mode currently
SET execution.runtime-mode = batch;
UPDATE T SET v1 = 4.0 WHERE k = 2;
SELECT * FROM T WHERE k = 2;
 -- Output:
+----+-----+----+
| k  | v1  | v2 |
+----+-----+----+
| 2  | 4.0 | t2 |
+----+-----+----+


-- Partial Update
INSERT INTO T(k, v1) VALUES (3, 3.0); -- set v1 to 3.0
SELECT * FROM T WHERE k = 3;
-- Output:
+----+-----+------+
| k  | v1  | v2   |
+----+-----+------+
| 3  | 3.0 | null |
+----+-----+------+
INSERT INTO T(k, v2) VALUES (3, 't3'); -- set v2 to 't3'
SELECT * FROM T WHERE k = 3;
-- Output:
+----+-----+----+
| k  | v1  | v2 |
+----+-----+----+
| 3  | 3.0 | t3 |
+----+-----+----+
 
-- Delete
DELETE FROM T WHERE k = 2;
-- Switch to streaming mode
SET execution.runtime-mode = streaming;
SELECT * FROM T;
-- Output:
+----+-----+----+
| k  | v1  | v2 |
+----+-----+----+
| 1  | 1.0 | t2 |
+----+-----+----+
```