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

-- Run setup.sql first. This example uses fixed names and demonstrates both a
-- Log Table and a Primary Key Table with table.datalake.enabled = true.

CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:19123',
    'paimon.s3.access-key' = 'rustfsadmin',
    'paimon.s3.secret-key' = 'rustfsadmin'
);

USE CATALOG fluss_catalog;

SET 'execution.runtime-mode' = 'batch';

-- Default read: in batch mode this is a bounded Batch Union Read. It combines
-- the latest lake snapshot with the Fluss log records after that snapshot.
SELECT * FROM lake_events;
SELECT * FROM lake_profiles;

-- Streaming Union Read: run this section separately after the batch queries.
-- It reads lake history first, then continues with new Fluss records.
-- SET 'execution.runtime-mode' = 'streaming';
-- SELECT * FROM lake_events;
-- SELECT * FROM lake_profiles;
