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

-- Run this file once to prepare the tables used by the other examples.
-- The table names are intentionally stable so the following examples can
-- refer to them directly.

CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:19123'
);

USE CATALOG fluss_catalog;

CREATE TABLE events (
    event_id BIGINT,
    tenant_id BIGINT,
    event_type STRING,
    payload STRING,
    event_time TIMESTAMP(3)
);

CREATE TABLE profiles (
    tenant_id BIGINT,
    user_id BIGINT,
    user_name STRING,
    user_level STRING,
    PRIMARY KEY (tenant_id, user_id) NOT ENFORCED
) WITH (
    'bucket.key' = 'tenant_id',
    'bucket.num' = '1'
);

CREATE TABLE orders (
    order_id BIGINT,
    tenant_id BIGINT,
    user_id BIGINT,
    amount DECIMAL(10, 2),
    ptime AS PROCTIME()
);

SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';

INSERT INTO events VALUES
    (1, 10, 'login', 'alpha', TIMESTAMP '2026-01-01 00:00:00'),
    (2, 10, 'purchase', 'beta', TIMESTAMP '2026-01-01 00:01:00'),
    (3, 20, 'login', 'gamma', TIMESTAMP '2026-01-01 00:02:00');

INSERT INTO profiles VALUES
    (10, 100, 'Alice', 'gold'),
    (10, 101, 'Bob', 'silver'),
    (20, 200, 'Carol', 'gold');

INSERT INTO orders VALUES
    (1000, 10, 100, 12.50),
    (1001, 10, 101, 25.00),
    (1002, 20, 200, 7.50);
