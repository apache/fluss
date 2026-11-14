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

package org.apache.fluss.fs.oss;

import org.apache.fluss.fs.FileSystemFailure;
import org.apache.fluss.fs.FileSystemOperationException;
import org.apache.fluss.fs.FileSystemPathNotFoundException;
import org.apache.fluss.fs.FsPath;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OSSFileSystemFailureTest {

    private static final FsPath TARGET = new FsPath("oss://bucket/table/partition");

    @Test
    void bucketNotFoundCannotBeMistakenForADeletedDirectory() throws IOException {
        OSSException serviceFailure = mock(OSSException.class);
        when(serviceFailure.getErrorCode()).thenReturn("NoSuchBucket");
        when(serviceFailure.getRequestId()).thenReturn("request-1");
        OSSFileSystem fs =
                failingFileSystem(new FileNotFoundException("missing bucket"), serviceFailure);

        assertThatThrownBy(() -> fs.listStatus(TARGET))
                .isInstanceOf(FileSystemOperationException.class)
                .satisfies(
                        failure -> {
                            FileSystemFailure normalized = (FileSystemFailure) failure;
                            assertThat(normalized.kind())
                                    .isEqualTo(FileSystemFailure.Kind.NOT_FOUND);
                            assertThat(normalized.resource())
                                    .isEqualTo(FileSystemFailure.Resource.ROOT);
                            assertThat(normalized.requestId()).isEqualTo("request-1");
                        });
    }

    @Test
    void objectNotFoundPreservesThePathNotFoundContract() throws IOException {
        OSSException serviceFailure = mock(OSSException.class);
        when(serviceFailure.getErrorCode()).thenReturn("NoSuchKey");
        OSSFileSystem fs = failingFileSystem(new IOException("missing key"), serviceFailure);

        assertThatThrownBy(() -> fs.open(TARGET))
                .isInstanceOf(FileSystemPathNotFoundException.class)
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void missingDirectoryMarkerDoesNotProveAnOssPrefixDisappeared() throws IOException {
        OSSException serviceFailure = mock(OSSException.class);
        when(serviceFailure.getErrorCode()).thenReturn("NoSuchKey");
        OSSFileSystem fs = failingFileSystem(new IOException("missing key"), serviceFailure);

        assertThatThrownBy(() -> fs.listStatus(TARGET))
                .isInstanceOf(FileSystemOperationException.class)
                .satisfies(
                        failure -> {
                            FileSystemFailure normalized = (FileSystemFailure) failure;
                            assertThat(normalized.kind())
                                    .isEqualTo(FileSystemFailure.Kind.NOT_FOUND);
                            assertThat(normalized.resource())
                                    .isEqualTo(FileSystemFailure.Resource.UNKNOWN);
                        });
    }

    @Test
    void ambiguousHadoopNotFoundDoesNotBecomeAPathNotFound() throws IOException {
        OSSFileSystem fs = failingFileSystem(new FileNotFoundException("ambiguous"), null);

        assertThatThrownBy(() -> fs.listStatus(TARGET))
                .isInstanceOf(FileSystemOperationException.class)
                .satisfies(
                        failure ->
                                assertThat(((FileSystemFailure) failure).kind())
                                        .isEqualTo(FileSystemFailure.Kind.UNEXPECTED));
    }

    @Test
    void rateLimitIsIdentifiedAsTransient() throws IOException {
        OSSException serviceFailure = mock(OSSException.class);
        when(serviceFailure.getErrorCode()).thenReturn("TotalQpsLimitExceeded");
        OSSFileSystem fs = failingFileSystem(new IOException("throttled"), serviceFailure);

        assertThatThrownBy(() -> fs.listStatus(TARGET))
                .isInstanceOf(FileSystemOperationException.class)
                .satisfies(
                        failure -> {
                            FileSystemFailure normalized = (FileSystemFailure) failure;
                            assertThat(normalized.kind())
                                    .isEqualTo(FileSystemFailure.Kind.RATE_LIMITED);
                            assertThat(normalized.isTemporary()).isTrue();
                        });
    }

    @Test
    void unavailableServiceIsTransientWithoutClaimingPathAbsence() throws IOException {
        OSSException serviceFailure = mock(OSSException.class);
        when(serviceFailure.getErrorCode()).thenReturn("ServiceUnavailable");
        OSSFileSystem fs = failingFileSystem(new IOException("unavailable"), serviceFailure);

        assertThatThrownBy(() -> fs.listStatus(TARGET))
                .isInstanceOf(FileSystemOperationException.class)
                .satisfies(
                        failure -> {
                            FileSystemFailure normalized = (FileSystemFailure) failure;
                            assertThat(normalized.kind())
                                    .isEqualTo(FileSystemFailure.Kind.UNEXPECTED);
                            assertThat(normalized.resource())
                                    .isEqualTo(FileSystemFailure.Resource.UNKNOWN);
                            assertThat(normalized.isTemporary()).isTrue();
                        });
    }

    @Test
    void nativeServiceFailureIsNormalizedAtTheOperationBoundary() throws IOException {
        OSSException serviceFailure = mock(OSSException.class);
        when(serviceFailure.getErrorCode()).thenReturn("NoSuchBucket");
        when(serviceFailure.getRequestId()).thenReturn("request-2");
        OSSFileSystem fs = failingFileSystem(serviceFailure);

        assertThatThrownBy(() -> fs.listStatus(TARGET))
                .isInstanceOf(FileSystemOperationException.class)
                .hasCause(serviceFailure)
                .satisfies(
                        failure -> {
                            FileSystemFailure normalized = (FileSystemFailure) failure;
                            assertThat(normalized.kind())
                                    .isEqualTo(FileSystemFailure.Kind.NOT_FOUND);
                            assertThat(normalized.resource())
                                    .isEqualTo(FileSystemFailure.Resource.ROOT);
                            assertThat(normalized.requestId()).isEqualTo("request-2");
                        });
    }

    @Test
    void nativeRateLimitIsNormalizedAtTheOperationBoundary() throws IOException {
        OSSException serviceFailure = mock(OSSException.class);
        when(serviceFailure.getErrorCode()).thenReturn("TotalQpsLimitExceeded");
        OSSFileSystem fs = failingFileSystem(serviceFailure);

        assertThatThrownBy(() -> fs.listStatus(TARGET))
                .isInstanceOf(FileSystemOperationException.class)
                .hasCause(serviceFailure)
                .satisfies(
                        failure -> {
                            FileSystemFailure normalized = (FileSystemFailure) failure;
                            assertThat(normalized.kind())
                                    .isEqualTo(FileSystemFailure.Kind.RATE_LIMITED);
                            assertThat(normalized.isTemporary()).isTrue();
                        });
    }

    @Test
    void nativePermissionFailureIsNotReportedAsMissing() throws IOException {
        OSSException serviceFailure = mock(OSSException.class);
        when(serviceFailure.getErrorCode()).thenReturn("AccessDenied");
        OSSFileSystem fs = failingFileSystem(serviceFailure);

        assertThatThrownBy(() -> fs.listStatus(TARGET))
                .isInstanceOf(FileSystemOperationException.class)
                .hasCause(serviceFailure)
                .satisfies(
                        failure ->
                                assertThat(((FileSystemFailure) failure).kind())
                                        .isEqualTo(FileSystemFailure.Kind.PERMISSION_DENIED));
    }

    @Test
    void nativeMissingObjectPreservesOpenContract() throws IOException {
        OSSException serviceFailure = mock(OSSException.class);
        when(serviceFailure.getErrorCode()).thenReturn("NoSuchKey");
        OSSFileSystem fs = failingFileSystem(serviceFailure);

        assertThatThrownBy(() -> fs.open(TARGET))
                .isInstanceOf(FileSystemPathNotFoundException.class)
                .hasCause(serviceFailure);
    }

    @Test
    void nativeClientFailureIsRetainedWithoutClaimingPathAbsence() throws IOException {
        ClientException clientFailure = mock(ClientException.class);
        when(clientFailure.getErrorCode()).thenReturn("RequestError");
        OSSFileSystem fs = failingFileSystem(clientFailure);

        assertThatThrownBy(() -> fs.listStatus(TARGET))
                .isInstanceOf(FileSystemOperationException.class)
                .hasCause(clientFailure)
                .satisfies(
                        failure -> {
                            FileSystemFailure normalized = (FileSystemFailure) failure;
                            assertThat(normalized.kind())
                                    .isEqualTo(FileSystemFailure.Kind.UNEXPECTED);
                            assertThat(normalized.resource())
                                    .isEqualTo(FileSystemFailure.Resource.UNKNOWN);
                            assertThat(normalized.serviceCode()).isEqualTo("RequestError");
                        });
    }

    @Test
    void unrelatedRuntimeFailureIsNotMisclassified() throws IOException {
        IllegalStateException failure = new IllegalStateException("unexpected state");
        OSSFileSystem fs = failingFileSystem(failure);

        assertThatThrownBy(() -> fs.listStatus(TARGET)).isSameAs(failure);
    }

    private static OSSFileSystem failingFileSystem(IOException failure, OSSException serviceFailure)
            throws IOException {
        if (serviceFailure != null) {
            failure.initCause(serviceFailure);
        }
        FileSystem hadoop = mock(FileSystem.class);
        when(hadoop.listStatus(any(Path.class))).thenThrow(failure);
        when(hadoop.open(any(Path.class))).thenThrow(failure);
        return new OSSFileSystem(hadoop, "oss", new Configuration());
    }

    private static OSSFileSystem failingFileSystem(RuntimeException failure) throws IOException {
        FileSystem hadoop = mock(FileSystem.class);
        when(hadoop.listStatus(any(Path.class))).thenThrow(failure);
        when(hadoop.open(any(Path.class))).thenThrow(failure);
        return new OSSFileSystem(hadoop, "oss", new Configuration());
    }
}
