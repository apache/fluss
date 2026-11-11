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

INSERT INTO embedding_events VALUES
    (1001, 101, ARRAY[CAST(0.12 AS FLOAT), CAST(0.34 AS FLOAT), CAST(0.56 AS FLOAT)], 'model-v1', TIMESTAMP '2026-10-07 10:00:00.000'),
    (1002, 102, ARRAY[CAST(0.21 AS FLOAT), CAST(0.43 AS FLOAT), CAST(0.65 AS FLOAT)], 'model-v1', TIMESTAMP '2026-10-07 10:00:01.000'),
    (1003, 101, ARRAY[CAST(0.14 AS FLOAT), CAST(0.36 AS FLOAT), CAST(0.58 AS FLOAT)], 'model-v2', TIMESTAMP '2026-10-07 10:00:02.000');
