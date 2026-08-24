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

-- Run this file once after `just up paimon`. The tables are intentionally
-- named for the following union-read examples.

CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:19123',
    'paimon.s3.access-key' = 'rustfsadmin',
    'paimon.s3.secret-key' = 'rustfsadmin'
);

USE CATALOG fluss_catalog;

CREATE TABLE lake_events (
    event_id BIGINT,
    event_type STRING,
    payload STRING
) WITH (
    'bucket.num' = '1',
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '5s'
);

CREATE TABLE lake_profiles (
    id BIGINT,
    name STRING,
    level STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'bucket.num' = '1',
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '5s'
);

SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';

INSERT INTO lake_events VALUES
    (1, 'login', 'alpha'),
    (2, 'purchase', 'beta');

INSERT INTO lake_profiles VALUES
    (1, 'Alice', 'gold'),
    (2, 'Bob', 'silver');
