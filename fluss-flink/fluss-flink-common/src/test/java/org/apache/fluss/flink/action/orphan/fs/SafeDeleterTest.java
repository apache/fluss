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

package org.apache.fluss.flink.action.orphan.fs;

import org.apache.fluss.flink.action.orphan.audit.AuditLogger;
import org.apache.fluss.flink.action.orphan.rule.Decision;
import org.apache.fluss.flink.action.orphan.rule.RuleId;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.local.LocalFileSystem;
import org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.RateLimiter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SafeDeleter} against the local filesystem. */
class SafeDeleterTest {

    @TempDir Path tmp;

    @Test
    void deleteFileRespectsDryRun() throws IOException {
        Path target = Files.createFile(tmp.resolve("orphan.log"));
        SafeDeleter d = new SafeDeleter(localFs(), true, new AuditLogger());
        d.deleteFile(new FsPath(target.toString()), Decision.DELETE, RuleId.LOG_SEGMENT);
        assertThat(Files.exists(target)).isTrue();
    }

    @Test
    void deleteFileActuallyDeletesWhenNotDryRun() throws IOException {
        Path target = Files.createFile(tmp.resolve("orphan.log"));
        SafeDeleter d = new SafeDeleter(localFs(), false, new AuditLogger());
        d.deleteFile(new FsPath(target.toString()), Decision.DELETE, RuleId.LOG_SEGMENT);
        assertThat(Files.exists(target)).isFalse();
    }

    @Test
    void deleteFileRejectsNonDeleteDecision() {
        SafeDeleter d = new SafeDeleter(null, false, new AuditLogger());
        assertThatThrownBy(
                        () ->
                                d.deleteFile(
                                        new FsPath("/tmp/x"), Decision.KEEP_ACTIVE, RuleId.UNKNOWN))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void deleteEmptyDirNoOpsOnNonEmpty() throws IOException {
        Path dir = Files.createDirectory(tmp.resolve("d"));
        Files.createFile(dir.resolve("child"));
        SafeDeleter d = new SafeDeleter(localFs(), false, new AuditLogger());
        d.deleteEmptyDir(new FsPath(dir.toString()));
        assertThat(Files.exists(dir)).isTrue();
    }

    @Test
    void deleteEmptyDirActuallyDeletes() throws IOException {
        Path dir = Files.createDirectory(tmp.resolve("d"));
        SafeDeleter d = new SafeDeleter(localFs(), false, new AuditLogger());
        d.deleteEmptyDir(new FsPath(dir.toString()));
        assertThat(Files.exists(dir)).isFalse();
    }

    @Test
    void multipleDeletesAllSucceed() throws IOException {
        Path a = Files.createFile(tmp.resolve("a.log"));
        Path b = Files.createFile(tmp.resolve("b.log"));
        Path c = Files.createFile(tmp.resolve("c.log"));
        Files.write(a, new byte[] {1});
        Files.write(b, new byte[] {2});
        Files.write(c, new byte[] {3});
        Path emptyDir = Files.createDirectory(tmp.resolve("emptyDir"));

        RateLimiter limiter = RateLimiter.create(Double.MAX_VALUE);
        SafeDeleter deleter = new SafeDeleter(localFs(), false, new AuditLogger(), limiter);

        deleter.deleteFile(new FsPath(a.toString()), Decision.DELETE, RuleId.LOG_SEGMENT);
        deleter.deleteFile(new FsPath(b.toString()), Decision.DELETE, RuleId.LOG_SEGMENT);
        deleter.deleteFile(new FsPath(c.toString()), Decision.DELETE, RuleId.LOG_SEGMENT);
        deleter.deleteEmptyDir(new FsPath(emptyDir.toString()));

        assertThat(Files.exists(a)).isFalse();
        assertThat(Files.exists(b)).isFalse();
        assertThat(Files.exists(c)).isFalse();
        assertThat(Files.exists(emptyDir)).isFalse();
    }

    @Test
    void dryRunPreservesAllFiles() throws IOException {
        Path file = Files.createFile(tmp.resolve("orphan.log"));
        Path emptyDir = Files.createDirectory(tmp.resolve("emptyDir"));

        RateLimiter limiter = RateLimiter.create(Double.MAX_VALUE);
        SafeDeleter deleter = new SafeDeleter(localFs(), true, new AuditLogger(), limiter);

        deleter.deleteFile(new FsPath(file.toString()), Decision.DELETE, RuleId.LOG_SEGMENT);
        deleter.deleteEmptyDir(new FsPath(emptyDir.toString()));

        assertThat(Files.exists(file)).isTrue();
        assertThat(Files.exists(emptyDir)).isTrue();
    }

    private static FileSystem localFs() {
        return LocalFileSystem.getSharedInstance();
    }
}
