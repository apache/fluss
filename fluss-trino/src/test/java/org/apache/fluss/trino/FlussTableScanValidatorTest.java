/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.trino;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.UNSUPPORTED_TABLE_TYPE;
import static org.apache.fluss.trino.FlussTableScanValidator.validateSplit;
import static org.apache.fluss.trino.FlussTableScanValidator.validateTable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests native table capabilities and physical split identity validation. */
final class FlussTableScanValidatorTest {
    private static final FlussTableHandle HANDLE =
            new FlussTableHandle("sales", "users", "sales", "users", 42, 1);

    @Test
    void testNativeTablesAndMatchingSplits() {
        for (boolean primaryKey : new boolean[] {false, true}) {
            for (boolean partitioned : new boolean[] {false, true}) {
                TableInfo info = tableInfo(primaryKey, partitioned, false);
                validateTable(HANDLE, info);
                FlussBucketHandle bucket =
                        new FlussBucketHandle(
                                42, partitioned ? Optional.of(7L) : Optional.empty(), 0);
                validateSplit(
                        primaryKey ? FlussSplit.forKv(bucket) : FlussSplit.forLog(bucket, 0, 1),
                        info);
            }
        }
    }

    @Test
    void testMismatchedScanTypes() {
        FlussBucketHandle bucket = new FlussBucketHandle(42, Optional.empty(), 0);
        assertInvalidSplit(FlussSplit.forKv(bucket), tableInfo(false, false, false), "scan type");
        assertInvalidSplit(
                FlussSplit.forLog(bucket, 0, 1), tableInfo(true, false, false), "scan type");
    }

    @Test
    void testMismatchedPhysicalIdentityAndPartitionLayout() {
        assertInvalidSplit(
                FlussSplit.forKv(new FlussBucketHandle(43, Optional.empty(), 0)),
                tableInfo(true, false, false),
                "table ID");
        assertInvalidSplit(
                FlussSplit.forKv(new FlussBucketHandle(42, Optional.of(7L), 0)),
                tableInfo(true, false, false),
                "partition layout");
        assertInvalidSplit(
                FlussSplit.forKv(new FlussBucketHandle(42, Optional.empty(), 0)),
                tableInfo(true, true, false),
                "partition layout");
    }

    @Test
    void testLakehouseTablesRemainUnsupported() {
        for (boolean primaryKey : new boolean[] {false, true}) {
            assertThatThrownBy(() -> validateTable(HANDLE, tableInfo(primaryKey, false, true)))
                    .isInstanceOfSatisfying(
                            TrinoException.class,
                            failure ->
                                    assertThat(failure.getErrorCode())
                                            .isEqualTo(UNSUPPORTED_TABLE_TYPE.toErrorCode()))
                    .hasMessageContaining("Lakehouse");
        }
    }

    private static void assertInvalidSplit(FlussSplit split, TableInfo info, String message) {
        assertThatThrownBy(() -> validateSplit(split, info))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        failure ->
                                assertThat(failure.getErrorCode())
                                        .isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode()))
                .hasMessageContaining(message);
    }

    private static TableInfo tableInfo(boolean primaryKey, boolean partitioned, boolean lakehouse) {
        Schema.Builder schema =
                Schema.newBuilder().column("id", DataTypes.INT()).column("region", DataTypes.INT());
        if (primaryKey) {
            schema.primaryKey("id", "region");
        }
        TableDescriptor.Builder descriptor =
                TableDescriptor.builder().schema(schema.build()).distributedBy(1);
        if (partitioned) {
            descriptor.partitionedBy("id");
        }
        if (lakehouse) {
            descriptor.property("table.datalake.enabled", "true");
        }
        return TableInfo.of(TablePath.of("sales", "users"), 42, 1, descriptor.build(), null, 0, 0);
    }
}
