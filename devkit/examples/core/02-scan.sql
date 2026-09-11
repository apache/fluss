-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements. See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership. The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License. You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:19123'
);

USE CATALOG fluss_catalog;

-- Batch scan: reads the current bounded contents and then exits.
SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
SET 'sql-client.execution.result-mode' = 'tableau';

SELECT * FROM events
/*+ OPTIONS('scan.startup.mode' = 'earliest') */;

SELECT * FROM profiles
/*+ OPTIONS('scan.startup.mode' = 'earliest') */;

-- Point lookup: a complete primary-key predicate returns one current row.
SELECT * FROM profiles
WHERE tenant_id = 10 AND user_id = 100;

-- Streaming scan: run this statement separately. It remains running and
-- continues to receive records appended after the job starts.
-- SET 'execution.runtime-mode' = 'streaming';
-- SELECT * FROM events
-- /*+ OPTIONS('scan.startup.mode' = 'earliest') */;
