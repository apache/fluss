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

package org.apache.fluss.client.write;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Tests the partition creation result used by {@link WriterClient}. */
class DynamicPartitionCreatorTest {
    private static final TablePath TABLE_PATH = TablePath.of("db", "partition_creator");
    private static final PhysicalTablePath PARTITION_PATH = PhysicalTablePath.of(TABLE_PATH, "p");
    private static final PartitionSpec PARTITION_SPEC =
            new PartitionSpec(Collections.singletonMap("pt", "p"));
    private static final TableInfo TABLE_INFO =
            TableInfo.of(
                    TABLE_PATH,
                    1,
                    1,
                    TableDescriptor.builder()
                            .schema(
                                    Schema.newBuilder()
                                            .column("id", DataTypes.INT())
                                            .column("pt", DataTypes.STRING())
                                            .build())
                            .distributedBy(1)
                            .partitionedBy("pt")
                            .build(),
                    "/tmp/fluss-partition-creator",
                    0,
                    0,
                    1);

    @Test
    void testCachedPartitionNeedsNoCreation() {
        MetadataUpdater updater = mock(MetadataUpdater.class);
        Admin admin = mock(Admin.class);
        when(updater.getPartitionId(PARTITION_PATH)).thenReturn(Optional.of(2L));

        for (boolean enabled : new boolean[] {false, true}) {
            DynamicPartitionCreator creator =
                    new DynamicPartitionCreator(updater, admin, enabled, ignored -> {});
            assertThat(creator.checkAndCreatePartitionAsync(PARTITION_PATH, TABLE_INFO)).isFalse();
        }
        verifyNoInteractions(admin);
        verify(updater, never()).checkAndUpdatePartitionMetadata(PARTITION_PATH);
    }

    @Test
    void testMissingPartitionWithCreationDisabledFails() throws Exception {
        MetadataUpdater updater = mock(MetadataUpdater.class);
        Admin admin = mock(Admin.class);
        when(updater.getPartitionId(PARTITION_PATH)).thenReturn(Optional.empty());
        when(updater.checkAndUpdatePartitionMetadata(PARTITION_PATH))
                .thenThrow(new PartitionNotExistException("missing"));

        DynamicPartitionCreator creator =
                new DynamicPartitionCreator(updater, admin, false, ignored -> {});
        assertThatThrownBy(() -> creator.checkAndCreatePartitionAsync(PARTITION_PATH, TABLE_INFO))
                .isInstanceOf(PartitionNotExistException.class);
        verifyNoInteractions(admin);
    }

    @Test
    void testCompletedCreationStillReportsCreation() throws Exception {
        MetadataUpdater updater = missingPartitionUpdater();
        Admin admin = mock(Admin.class);
        when(admin.createPartition(TABLE_PATH, PARTITION_SPEC, true))
                .thenReturn(CompletableFuture.completedFuture(null));
        DynamicPartitionCreator creator =
                new DynamicPartitionCreator(updater, admin, true, ignored -> {});

        assertThat(creator.checkAndCreatePartitionAsync(PARTITION_PATH, TABLE_INFO)).isTrue();
        verify(admin).createPartition(TABLE_PATH, PARTITION_SPEC, true);
    }

    @Test
    void testPendingCreationIsObservedWithoutDuplicateCreate() throws Exception {
        MetadataUpdater updater = missingPartitionUpdater();
        Admin admin = mock(Admin.class);
        CompletableFuture<Void> pending = new CompletableFuture<>();
        when(admin.createPartition(TABLE_PATH, PARTITION_SPEC, true)).thenReturn(pending);
        DynamicPartitionCreator creator =
                new DynamicPartitionCreator(updater, admin, true, ignored -> {});

        assertThat(creator.checkAndCreatePartitionAsync(PARTITION_PATH, TABLE_INFO)).isTrue();
        assertThat(creator.checkAndCreatePartitionAsync(PARTITION_PATH, TABLE_INFO)).isTrue();
        verify(admin).createPartition(TABLE_PATH, PARTITION_SPEC, true);
        pending.complete(null);
    }

    private static MetadataUpdater missingPartitionUpdater() throws Exception {
        MetadataUpdater updater = mock(MetadataUpdater.class);
        when(updater.getPartitionId(PARTITION_PATH)).thenReturn(Optional.empty());
        when(updater.checkAndUpdatePartitionMetadata(PARTITION_PATH)).thenReturn(false);
        return updater;
    }
}
