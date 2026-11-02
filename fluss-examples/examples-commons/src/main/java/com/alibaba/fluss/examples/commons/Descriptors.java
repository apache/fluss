/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.examples.commons;

import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.types.DataTypes;

/** descriptors to create table for usage in examples. This is a common place to put. */
public class Descriptors {
    public static final String DB_NAME = "examples_db";
    public static final String LOG_TABLE_BASIC = "log_basic";
    public static final String PRIMARYKEY_TABLE_BASIC = "primarykey_basic";
    public static final String PRIMARYKEY_MULTKEY_TABLE = "primarykey_multkeys";

    public static final DatabaseDescriptor SAMPLE_DB_DESCRIPTOR =
            DatabaseDescriptor.builder().build();

    public static final Schema LOG_TABLE_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.BIGINT())
                    .column("name", DataTypes.STRING())
                    .build();

    public static final Schema PRIMARYKEY_TABLE_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.BIGINT())
                    .column("name", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    public static final Schema PRIMARYKEY_MULTKEY_TABLE_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.BIGINT())
                    .column("ref_id", DataTypes.STRING())
                    .column("name", DataTypes.STRING())
                    .column("updated_at", DataTypes.TIMESTAMP())
                    .primaryKey("ref_id", "id")
                    .build();

    public static final TableDescriptor LOG_TABLE_BASIC_DESCRIPTOR =
            TableDescriptor.builder().schema(LOG_TABLE_SCHEMA).distributedBy(3, "id").build();

    public static final TableDescriptor PRIMARYKEY_TABLE_BASIC_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(PRIMARYKEY_TABLE_SCHEMA)
                    .distributedBy(3, "id")
                    .build();

    public static final TableDescriptor PRIMARYKEY_MULTKEY_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(PRIMARYKEY_MULTKEY_TABLE_SCHEMA)
                    .distributedBy(3, "ref_id")
                    .build();

    public static final TablePath LOG_TABLE_BASIC_PATH = TablePath.of(DB_NAME, LOG_TABLE_BASIC);

    public static final TablePath PRIMARYKEY_TABLE_BASIC_PATH =
            TablePath.of(DB_NAME, PRIMARYKEY_TABLE_BASIC);

    public static final TablePath PRIMARYKEY_MULTKEY_TABLE_PATH =
            TablePath.of(DB_NAME, PRIMARYKEY_MULTKEY_TABLE);
}
