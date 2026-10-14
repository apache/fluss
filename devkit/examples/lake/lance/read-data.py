# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time

import lance


for attempt in range(30):
    try:
        dataset = lance.dataset(
            "s3://fluss/lance/fluss/embedding_events.lance",
            storage_options={
                "aws_access_key_id": "rustfsadmin",
                "aws_secret_access_key": "rustfsadmin",
                "aws_endpoint": "http://127.0.0.1:9000",
                "allow_http": "true",
            },
        )
        table = dataset.to_table()
        assert table.num_rows == 3, table.num_rows
        assert sorted(table.column("event_id").to_pylist()) == [1001, 1002, 1003]
        assert table.column("item_id").to_pylist().count(101) == 2
        assert str(table.schema.field("embedding").type.value_type).lower() in (
            "float",
            "double",
        )
        print(table)
        break
    except Exception:
        if attempt == 29:
            raise
        time.sleep(2)
