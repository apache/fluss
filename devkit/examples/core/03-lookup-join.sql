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

-- Run one section at a time. Both examples use the primary-key table as a
-- dimension table and write the result to a disposable Flink sink.
SET 'execution.runtime-mode' = 'streaming';

-- Point lookup: the join condition contains the complete primary key.
CREATE TEMPORARY TABLE point_lookup_sink (
    order_id BIGINT,
    user_name STRING,
    user_level STRING,
    amount DECIMAL(10, 2)
) WITH ('connector' = 'blackhole');

INSERT INTO point_lookup_sink
SELECT o.order_id, p.user_name, p.user_level, o.amount
FROM (SELECT *, PROCTIME() AS proc_time FROM orders) AS o
LEFT JOIN profiles
FOR SYSTEM_TIME AS OF o.proc_time AS p
ON o.tenant_id = p.tenant_id AND o.user_id = p.user_id;

-- Prefix lookup: comment the point-lookup block above before running this
-- block. The condition uses the bucket-key prefix and may return many rows.
-- CREATE TEMPORARY TABLE prefix_lookup_sink (
--     order_id BIGINT,
--     user_name STRING,
--     user_level STRING,
--     amount DECIMAL(10, 2)
-- ) WITH ('connector' = 'blackhole');
--
-- INSERT INTO prefix_lookup_sink
-- SELECT o.order_id, p.user_name, p.user_level, o.amount
-- FROM (SELECT *, PROCTIME() AS proc_time FROM orders) AS o
-- LEFT JOIN profiles
-- FOR SYSTEM_TIME AS OF o.proc_time AS p
-- ON o.tenant_id = p.tenant_id;
