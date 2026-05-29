# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Unit tests for Schema (no cluster required)."""

import pyarrow as pa

import fluss


def test_get_primary_keys():
    fields = pa.schema([
        pa.field("id", pa.int32()),
        pa.field("name", pa.string()),
    ])

    schema_with_pk = fluss.Schema(fields, primary_keys=["id"])
    assert schema_with_pk.get_primary_keys() == ["id"]

    schema_without_pk = fluss.Schema(fields)
    assert schema_without_pk.get_primary_keys() == []


def test_schema_with_array():
    # Test that a schema can be constructed from a pyarrow schema containing a list
    fields = pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field("tags", pa.list_(pa.string())),
        ]
    )
    schema = fluss.Schema(fields)
    assert schema.get_column_names() == ["id", "tags"]
    assert schema.get_column_types() == ["int", "array<string>"]


def test_nullable_fields():
    fields = pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("name", pa.string()),
        ]
    )
    schema = fluss.Schema(fields)
    assert schema.get_column_types() == ["int NOT NULL", "string"]
    assert schema.get_columns() == [("id", "int NOT NULL"), ("name", "string")]


def test_pk_forces_non_nullable():
    fields = pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field("name", pa.string()),
        ]
    )
    schema = fluss.Schema(fields, primary_keys=["id"])
    types = schema.get_column_types()
    assert types[0] == "int NOT NULL"
    assert types[1] == "string"


def test_nested_list_nullability():
    fields = pa.schema(
        [
            pa.field(
                "tags",
                pa.list_(pa.field("item", pa.string(), nullable=False)),
            ),
            pa.field("ids", pa.list_(pa.int32()), nullable=False),
            pa.field(
                "strict_ids",
                pa.list_(pa.field("item", pa.int32(), nullable=False)),
                nullable=False,
            ),
        ]
    )
    schema = fluss.Schema(fields)
    types = schema.get_column_types()
    assert types[0] == "array<string NOT NULL>"
    assert types[1] == "array<int> NOT NULL"
    assert types[2] == "array<int NOT NULL> NOT NULL"


