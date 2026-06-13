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

package org.apache.fluss.flink.action.orphan.job;

import org.apache.fluss.flink.action.orphan.audit.AuditLogger;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.RateLimiter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class EmptyDirSweeperTest {

    private static EmptyDirSweeper newSweeper(boolean dryRun) {
        return new EmptyDirSweeper(dryRun, new AuditLogger(), RateLimiter.create(1000.0));
    }

    @Test
    void deletesEmptyDirsBottomUp(@TempDir Path tmp) throws IOException {
        Path a = Files.createDirectories(tmp.resolve("a"));
        Path b = Files.createDirectories(a.resolve("b"));
        Path c = Files.createDirectories(b.resolve("c"));

        EmptyDirSweeper sweeper = newSweeper(false);
        sweeper.registerTouched(new FsPath(a.toString()));
        long removed = sweeper.sweep();

        assertThat(removed).isEqualTo(3L);
        assertThat(Files.exists(c)).isFalse();
        assertThat(Files.exists(b)).isFalse();
        assertThat(Files.exists(a)).isFalse();
    }

    @Test
    void leavesNonEmptyDirsAlone(@TempDir Path tmp) throws IOException {
        Path a = Files.createDirectories(tmp.resolve("a"));
        Path b = Files.createDirectories(a.resolve("b"));
        Files.write(b.resolve("keep.txt"), new byte[] {0x42});

        EmptyDirSweeper sweeper = newSweeper(false);
        sweeper.registerTouched(new FsPath(a.toString()));
        long removed = sweeper.sweep();

        assertThat(removed).isEqualTo(0L);
        assertThat(Files.exists(b)).isTrue();
        assertThat(Files.exists(a)).isTrue();
    }

    @Test
    void dryRunCountsWouldDeleteButDoesNotActuallyDelete(@TempDir Path tmp) throws IOException {
        Path a = Files.createDirectories(tmp.resolve("a"));
        Path b = Files.createDirectories(a.resolve("b"));

        EmptyDirSweeper sweeper = newSweeper(true /* dryRun */);
        sweeper.registerTouched(new FsPath(a.toString()));
        long removed = sweeper.sweep();

        // dry-run leaves both directories on disk, but reports the would-delete count.
        assertThat(removed).isEqualTo(2L);
        assertThat(Files.exists(b)).isTrue();
        assertThat(Files.exists(a)).isTrue();
    }

    @Test
    void nonExistentRootIsNoOp(@TempDir Path tmp) throws IOException {
        EmptyDirSweeper sweeper = newSweeper(false);
        sweeper.registerTouched(new FsPath(tmp.resolve("does-not-exist").toString()));
        assertThat(sweeper.sweep()).isEqualTo(0L);
    }
}
