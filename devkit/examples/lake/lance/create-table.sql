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

CREATE TABLE embedding_events (
    event_id BIGINT,
    item_id BIGINT,
    embedding ARRAY<FLOAT>,
    model_version STRING,
    event_time TIMESTAMP(3)
) WITH (
    'bucket.num' = '1',
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '5s',
    'lance.max_row_per_file' = '512'
);
