/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.utils;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.entity.TablePropertyChanges;

import org.junit.jupiter.api.Test;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.server.utils.TableDescriptorValidation.validateAlterTableProperties;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link TableDescriptorValidation}. */
class TableDescriptorValidationTest {

    @Test
    void testValidateAlterBucketNumber() {
        TablePath tablePath = TablePath.of("test_db", "test_table");
        TableInfo tableInfo = createLogTableInfo(tablePath, 10);

        // 1. reduce bucket number should throw exception
        TablePropertyChanges.Builder builder = TablePropertyChanges.builder();
        builder.setBucketNum(5);
        TablePropertyChanges reduceChanges = builder.build();
        assertThatThrownBy(() -> validateAlterTableProperties(tableInfo, reduceChanges))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("Bucket number can only be increased")
                .hasMessageContaining("current bucket number is 10")
                .hasMessageContaining("new bucket number is 5");

        // 2. increase bucket number should not throw exception
        builder = TablePropertyChanges.builder();
        builder.setBucketNum(20);
        TablePropertyChanges increaseChanges = builder.build();
        assertThatNoException()
                .isThrownBy(() -> validateAlterTableProperties(tableInfo, increaseChanges));

        // 3. alter bucket number and properties at the same time should throw exception
        builder = TablePropertyChanges.builder();
        builder.setBucketNum(20);
        builder.setTableProperty(ConfigOptions.TABLE_TIERED_LOG_LOCAL_SEGMENTS.key(), "3");
        TablePropertyChanges mixedChanges = builder.build();
        assertThatThrownBy(() -> validateAlterTableProperties(tableInfo, mixedChanges))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessage("Cannot alter table properties and bucket number at the same time.");
    }

    private TableInfo createLogTableInfo(TablePath tablePath, int numBuckets) {
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA)
                        .distributedBy(numBuckets)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                        .build();
        return TableInfo.of(
                tablePath,
                1L,
                1,
                tableDescriptor,
                System.currentTimeMillis(),
                System.currentTimeMillis());
    }
}
