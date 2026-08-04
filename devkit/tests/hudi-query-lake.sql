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

SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'dfs',
    'catalog.path' = 's3a://fluss/hudi',
    'hadoop.fs.s3a.endpoint' = 'http://rustfs:9000',
    'hadoop.fs.s3a.access.key' = 'rustfsadmin',
    'hadoop.fs.s3a.secret.key' = 'rustfsadmin',
    'hadoop.fs.s3a.path.style.access' = 'true',
    'hadoop.fs.s3a.connection.ssl.enabled' = 'false',
    'hadoop.fs.s3a.impl.disable.cache' = 'true'
);

USE CATALOG hudi_catalog;

SELECT
    COUNT(*) AS row_count,
    SUM(id) AS id_sum,
    COUNT(DISTINCT payload) AS payload_count,
    MIN(payload) AS first_payload,
    MAX(payload) AS last_payload
FROM fluss.${DEVKIT_TABLE};
