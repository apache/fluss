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

package org.apache.fluss.server.lock;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.replica.ReplicaManager;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Unit tests for {@link ServerColumnLockService}. */
class ServerColumnLockServiceTest {

    private static final int SERVER_ID = 1;
    private static final long TABLE_ID = 100L;
    private static final long TABLE_ID_2 = 200L;
    private static final String OWNER_A = "owner-a";
    private static final String OWNER_B = "owner-b";
    private static final int SCHEMA_ID_V1 = 1;
    private static final int SCHEMA_ID_V2 = 2;
    private static final int[] COLUMNS_1_2 = new int[] {1, 2};
    private static final int[] COLUMNS_2_3 = new int[] {2, 3};
    private static final int[] COLUMNS_3_4 = new int[] {3, 4};
    private static final long DEFAULT_TTL_MS = 5000L;

    private ServerColumnLockService lockService;

    @BeforeEach
    void setup() {
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        when(replicaManager.getServerId()).thenReturn(SERVER_ID);
        lockService = new ServerColumnLockService(replicaManager);
    }

    @AfterEach
    void teardown() {
        if (lockService != null) {
            lockService.close();
        }
    }

    @Test
    void testAcquireLock_FirstTimeSuccess() {
        // Acquire lock for the first time should succeed
        ServerColumnLockService.TableLockRequest request =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_A, SCHEMA_ID_V1, COLUMNS_1_2, DEFAULT_TTL_MS);

        ServerColumnLockService.TableLockResult result = lockService.acquireLock(request);

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getTableId()).isEqualTo(TABLE_ID);
        assertThat(result.getAcquiredTime()).isGreaterThan(0);
        assertThat(result.getErrorMessage()).isNull();
    }

    @Test
    void testAcquireLock_SameOwnerSameColumns_SuccessAsRenewal() {
        // Owner A acquires lock
        ServerColumnLockService.TableLockRequest request1 =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_A, SCHEMA_ID_V1, COLUMNS_1_2, DEFAULT_TTL_MS);
        ServerColumnLockService.TableLockResult result1 = lockService.acquireLock(request1);
        assertThat(result1.isSuccess()).isTrue();

        // Owner A acquires same lock again (re-entry) - should succeed
        ServerColumnLockService.TableLockRequest request2 =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_A, SCHEMA_ID_V1, COLUMNS_1_2, DEFAULT_TTL_MS);
        ServerColumnLockService.TableLockResult result2 = lockService.acquireLock(request2);

        assertThat(result2.isSuccess()).isTrue();
        assertThat(result2.getTableId()).isEqualTo(TABLE_ID);
    }

    @Test
    void testAcquireLock_SameOwnerDifferentColumns_Conflict() {
        // Owner A acquires lock for columns [1, 2]
        ServerColumnLockService.TableLockRequest request1 =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_A, SCHEMA_ID_V1, COLUMNS_1_2, DEFAULT_TTL_MS);
        ServerColumnLockService.TableLockResult result1 = lockService.acquireLock(request1);
        assertThat(result1.isSuccess()).isTrue();

        // Owner A tries to acquire lock for columns [2, 3] - should fail (different columns)
        ServerColumnLockService.TableLockRequest request2 =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_A, SCHEMA_ID_V1, COLUMNS_2_3, DEFAULT_TTL_MS);
        ServerColumnLockService.TableLockResult result2 = lockService.acquireLock(request2);

        assertThat(result2.isSuccess()).isFalse();
        assertThat(result2.getErrorMessage()).contains("Column lock conflict");
    }

    @Test
    void testAcquireLock_DifferentOwnerOverlappingColumns_Conflict() {
        // Owner A acquires lock for columns [1, 2]
        ServerColumnLockService.TableLockRequest request1 =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_A, SCHEMA_ID_V1, COLUMNS_1_2, DEFAULT_TTL_MS);
        ServerColumnLockService.TableLockResult result1 = lockService.acquireLock(request1);
        assertThat(result1.isSuccess()).isTrue();

        // Owner B tries to acquire lock for columns [2, 3] - should fail (overlapping column 2)
        ServerColumnLockService.TableLockRequest request2 =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_B, SCHEMA_ID_V1, COLUMNS_2_3, DEFAULT_TTL_MS);
        ServerColumnLockService.TableLockResult result2 = lockService.acquireLock(request2);

        assertThat(result2.isSuccess()).isFalse();
        assertThat(result2.getErrorMessage()).contains("Column lock conflict");
    }

    @Test
    void testAcquireLock_DifferentOwnerNonOverlappingColumns_Success() {
        // Owner A acquires lock for columns [1, 2]
        ServerColumnLockService.TableLockRequest request1 =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_A, SCHEMA_ID_V1, COLUMNS_1_2, DEFAULT_TTL_MS);
        ServerColumnLockService.TableLockResult result1 = lockService.acquireLock(request1);
        assertThat(result1.isSuccess()).isTrue();

        // Owner B acquires lock for columns [3, 4] - should succeed (no overlap)
        ServerColumnLockService.TableLockRequest request2 =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_B, SCHEMA_ID_V1, COLUMNS_3_4, DEFAULT_TTL_MS);
        ServerColumnLockService.TableLockResult result2 = lockService.acquireLock(request2);

        assertThat(result2.isSuccess()).isTrue();
        assertThat(result2.getTableId()).isEqualTo(TABLE_ID);
    }

    @Test
    void testAcquireLock_DifferentSchema_NoConflict() {
        // Owner A acquires lock for schema v1, columns [1, 2]
        ServerColumnLockService.TableLockRequest request1 =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_A, SCHEMA_ID_V1, COLUMNS_1_2, DEFAULT_TTL_MS);
        ServerColumnLockService.TableLockResult result1 = lockService.acquireLock(request1);
        assertThat(result1.isSuccess()).isTrue();

        // Owner B acquires lock for schema v2, columns [1, 2] - should succeed (different schema)
        ServerColumnLockService.TableLockRequest request2 =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_B, SCHEMA_ID_V2, COLUMNS_1_2, DEFAULT_TTL_MS);
        ServerColumnLockService.TableLockResult result2 = lockService.acquireLock(request2);

        assertThat(result2.isSuccess()).isTrue();
        assertThat(result2.getTableId()).isEqualTo(TABLE_ID);
    }

    @Test
    void testAcquireLock_AllColumns_ConflictsWithAnySpecificColumns() {
        // Owner A acquires lock for all columns (null)
        ServerColumnLockService.TableLockRequest request1 =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_A, SCHEMA_ID_V1, null, DEFAULT_TTL_MS);
        ServerColumnLockService.TableLockResult result1 = lockService.acquireLock(request1);
        assertThat(result1.isSuccess()).isTrue();

        // Owner B tries to acquire lock for specific columns - should fail
        ServerColumnLockService.TableLockRequest request2 =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_B, SCHEMA_ID_V1, COLUMNS_1_2, DEFAULT_TTL_MS);
        ServerColumnLockService.TableLockResult result2 = lockService.acquireLock(request2);

        assertThat(result2.isSuccess()).isFalse();
        assertThat(result2.getErrorMessage()).contains("Column lock conflict");
    }

    @Test
    void testAcquireLock_DifferentTables_NoConflict() {
        // Owner A acquires lock for table 1
        ServerColumnLockService.TableLockRequest request1 =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_A, SCHEMA_ID_V1, COLUMNS_1_2, DEFAULT_TTL_MS);
        ServerColumnLockService.TableLockResult result1 = lockService.acquireLock(request1);
        assertThat(result1.isSuccess()).isTrue();

        // Owner B acquires lock for table 2 with same columns - should succeed (different table)
        ServerColumnLockService.TableLockRequest request2 =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID_2, OWNER_B, SCHEMA_ID_V1, COLUMNS_1_2, DEFAULT_TTL_MS);
        ServerColumnLockService.TableLockResult result2 = lockService.acquireLock(request2);

        assertThat(result2.isSuccess()).isTrue();
        assertThat(result2.getTableId()).isEqualTo(TABLE_ID_2);
    }

    @Test
    void testReleaseLock_Success() {
        // Acquire lock
        ServerColumnLockService.TableLockRequest request =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_A, SCHEMA_ID_V1, COLUMNS_1_2, DEFAULT_TTL_MS);
        ServerColumnLockService.TableLockResult acquireResult = lockService.acquireLock(request);
        assertThat(acquireResult.isSuccess()).isTrue();

        // Release lock
        ServerColumnLockService.TableLockResult releaseResult =
                lockService.releaseLockForTable(TABLE_ID, OWNER_A);

        assertThat(releaseResult.isSuccess()).isTrue();
        assertThat(releaseResult.getTableId()).isEqualTo(TABLE_ID);

        // Another owner should be able to acquire now
        ServerColumnLockService.TableLockRequest request2 =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_B, SCHEMA_ID_V1, COLUMNS_1_2, DEFAULT_TTL_MS);
        ServerColumnLockService.TableLockResult result2 = lockService.acquireLock(request2);
        assertThat(result2.isSuccess()).isTrue();
    }

    @Test
    void testRenewLock_Success() {
        // Acquire lock
        ServerColumnLockService.TableLockRequest request =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_A, SCHEMA_ID_V1, COLUMNS_1_2, DEFAULT_TTL_MS);
        ServerColumnLockService.TableLockResult acquireResult = lockService.acquireLock(request);
        assertThat(acquireResult.isSuccess()).isTrue();
        long originalAcquiredTime = acquireResult.getAcquiredTime();

        // Wait a bit before renewing
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Renew lock
        ServerColumnLockService.TableLockResult renewResult =
                lockService.renewLockForTable(TABLE_ID, OWNER_A);

        assertThat(renewResult.isSuccess()).isTrue();
        assertThat(renewResult.getTableId()).isEqualTo(TABLE_ID);
        assertThat(renewResult.getAcquiredTime()).isGreaterThan(originalAcquiredTime);
    }

    @Test
    void testRenewLock_NonExistentLock_Failure() {
        // Try to renew a lock that doesn't exist
        ServerColumnLockService.TableLockResult renewResult =
                lockService.renewLockForTable(TABLE_ID, OWNER_A);

        assertThat(renewResult.isSuccess()).isFalse();
        assertThat(renewResult.getErrorMessage()).contains("Lock not found or expired");
    }

    @Test
    void testAcquireLock_ExpiredLockIsCleanedUp() throws InterruptedException {
        // Acquire lock with very short TTL
        long shortTtl = 50L; // 50ms
        ServerColumnLockService.TableLockRequest request1 =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_A, SCHEMA_ID_V1, COLUMNS_1_2, shortTtl);
        ServerColumnLockService.TableLockResult result1 = lockService.acquireLock(request1);
        assertThat(result1.isSuccess()).isTrue();

        // Wait for lock to expire
        Thread.sleep(shortTtl + 50);

        // Another owner should be able to acquire now (expired lock cleaned up automatically)
        ServerColumnLockService.TableLockRequest request2 =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_B, SCHEMA_ID_V1, COLUMNS_1_2, DEFAULT_TTL_MS);
        ServerColumnLockService.TableLockResult result2 = lockService.acquireLock(request2);

        assertThat(result2.isSuccess()).isTrue();
    }

    @Test
    void testValidateWritePermission_NoLocksExist_AllowAllWrites() {
        TableBucket tableBucket = new TableBucket(TABLE_ID, 0);

        // No locks exist - all writes should be allowed
        boolean permitted =
                lockService.validateWritePermission(tableBucket, null, SCHEMA_ID_V1, COLUMNS_1_2);

        assertThat(permitted).isTrue();
    }

    @Test
    void testValidateWritePermission_LockExistsButNoOwnerProvided_Deny() {
        // Acquire lock
        ServerColumnLockService.TableLockRequest request =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_A, SCHEMA_ID_V1, COLUMNS_1_2, DEFAULT_TTL_MS);
        lockService.acquireLock(request);

        TableBucket tableBucket = new TableBucket(TABLE_ID, 0);

        // Write attempt without owner ID - should be denied
        boolean permitted =
                lockService.validateWritePermission(tableBucket, null, SCHEMA_ID_V1, COLUMNS_1_2);

        assertThat(permitted).isFalse();
    }

    @Test
    void testValidateWritePermission_CorrectOwnerAndColumns_Allow() {
        // Acquire lock
        ServerColumnLockService.TableLockRequest request =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_A, SCHEMA_ID_V1, COLUMNS_1_2, DEFAULT_TTL_MS);
        lockService.acquireLock(request);

        TableBucket tableBucket = new TableBucket(TABLE_ID, 0);

        // Write with correct owner and matching columns - should be allowed
        boolean permitted =
                lockService.validateWritePermission(
                        tableBucket, OWNER_A, SCHEMA_ID_V1, COLUMNS_1_2);

        assertThat(permitted).isTrue();
    }

    @Test
    void testValidateWritePermission_WrongOwner_Deny() {
        // Acquire lock with Owner A
        ServerColumnLockService.TableLockRequest request =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_A, SCHEMA_ID_V1, COLUMNS_1_2, DEFAULT_TTL_MS);
        lockService.acquireLock(request);

        TableBucket tableBucket = new TableBucket(TABLE_ID, 0);

        // Write with different owner - should be denied
        boolean permitted =
                lockService.validateWritePermission(
                        tableBucket, OWNER_B, SCHEMA_ID_V1, COLUMNS_1_2);

        assertThat(permitted).isFalse();
    }

    @Test
    void testValidateWritePermission_ColumnsNotCoveredByLock_Deny() {
        // Acquire lock for columns [1, 2]
        ServerColumnLockService.TableLockRequest request =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_A, SCHEMA_ID_V1, COLUMNS_1_2, DEFAULT_TTL_MS);
        lockService.acquireLock(request);

        TableBucket tableBucket = new TableBucket(TABLE_ID, 0);

        // Write to columns [2, 3] - not fully covered by lock - should be denied
        boolean permitted =
                lockService.validateWritePermission(
                        tableBucket, OWNER_A, SCHEMA_ID_V1, COLUMNS_2_3);

        assertThat(permitted).isFalse();
    }

    @Test
    void testValidateWritePermission_LockCoversAllColumns_AllowAnySubset() {
        // Acquire lock for all columns (null)
        ServerColumnLockService.TableLockRequest request =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_A, SCHEMA_ID_V1, null, DEFAULT_TTL_MS);
        lockService.acquireLock(request);

        TableBucket tableBucket = new TableBucket(TABLE_ID, 0);

        // Write to specific columns - should be allowed (lock covers all)
        boolean permitted =
                lockService.validateWritePermission(
                        tableBucket, OWNER_A, SCHEMA_ID_V1, COLUMNS_1_2);

        assertThat(permitted).isTrue();
    }

    @Test
    void testValidateWritePermission_ExpiredLock_Deny() throws InterruptedException {
        // Acquire lock with very short TTL
        long shortTtl = 50L;
        ServerColumnLockService.TableLockRequest request =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_A, SCHEMA_ID_V1, COLUMNS_1_2, shortTtl);
        lockService.acquireLock(request);

        // Wait for lock to expire
        Thread.sleep(shortTtl + 50);

        TableBucket tableBucket = new TableBucket(TABLE_ID, 0);

        // Write with expired lock - should be denied
        boolean permitted =
                lockService.validateWritePermission(
                        tableBucket, OWNER_A, SCHEMA_ID_V1, COLUMNS_1_2);

        assertThat(permitted).isFalse();
    }

    @Test
    void testHasActiveLocks_NoLocks_ReturnFalse() {
        TableBucket tableBucket = new TableBucket(TABLE_ID, 0);
        assertThat(lockService.hasActiveLocks(tableBucket)).isFalse();
    }

    @Test
    void testHasActiveLocks_WithActiveLock_ReturnTrue() {
        // Acquire lock
        ServerColumnLockService.TableLockRequest request =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_A, SCHEMA_ID_V1, COLUMNS_1_2, DEFAULT_TTL_MS);
        lockService.acquireLock(request);

        TableBucket tableBucket = new TableBucket(TABLE_ID, 0);
        assertThat(lockService.hasActiveLocks(tableBucket)).isTrue();
    }

    @Test
    void testHasActiveLocks_AfterRelease_ReturnFalse() {
        // Acquire lock
        ServerColumnLockService.TableLockRequest request =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_A, SCHEMA_ID_V1, COLUMNS_1_2, DEFAULT_TTL_MS);
        lockService.acquireLock(request);

        // Release lock
        lockService.releaseLockForTable(TABLE_ID, OWNER_A);

        TableBucket tableBucket = new TableBucket(TABLE_ID, 0);
        assertThat(lockService.hasActiveLocks(tableBucket)).isFalse();
    }

    @Test
    void testClose_RejectsNewRequests() {
        // Close the service
        lockService.close();

        // Try to acquire lock after close - should fail
        ServerColumnLockService.TableLockRequest request =
                new ServerColumnLockService.TableLockRequest(
                        TABLE_ID, OWNER_A, SCHEMA_ID_V1, COLUMNS_1_2, DEFAULT_TTL_MS);
        ServerColumnLockService.TableLockResult result = lockService.acquireLock(request);

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getErrorMessage()).contains("Service is closed");
    }
}
