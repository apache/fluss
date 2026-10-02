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

import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.RateLimiter;

import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FileSystemProbeTest {

    private static final FsPath DIR = new FsPath("file:/bucket/snap-1");

    @Test
    void confirmsAbsentPathsWhenListingReturnsNull() throws IOException {
        FileSystem fs = mock(FileSystem.class);
        when(fs.listStatus(DIR)).thenReturn((FileStatus[]) null);
        when(fs.getFileStatus(DIR)).thenThrow(new FileNotFoundException("gone"));
        assertThat(FileSystemProbe.listStatus(fs, DIR, RateLimiter.create(1000.0))).isEmpty();
        verify(fs, times(2)).listStatus(DIR);
        verify(fs, times(2)).getFileStatus(DIR);
    }

    @Test
    void rejectsNullListingOfAnExistingDirectory() throws IOException {
        FileSystem fs = mock(FileSystem.class);
        when(fs.listStatus(DIR)).thenReturn((FileStatus[]) null);
        when(fs.getFileStatus(DIR)).thenReturn(mock(FileStatus.class));
        assertThatThrownBy(() -> FileSystemProbe.listStatus(fs, DIR, RateLimiter.create(1000.0)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("returned null");
        verify(fs).listStatus(DIR);
    }

    @Test
    void retriesAListingThatRacesWithDirectoryRemoval() throws IOException {
        FileSystem fs = mock(FileSystem.class);
        FileStatus[] listing = new FileStatus[0];
        when(fs.listStatus(DIR)).thenThrow(new FileNotFoundException("raced")).thenReturn(listing);

        assertThat(FileSystemProbe.listStatus(fs, DIR, RateLimiter.create(1000.0)))
                .contains(listing);
        verify(fs, times(2)).listStatus(DIR);
    }

    @Test
    void treatsRepeatedNotFoundAsAnAbsentDirectory() throws IOException {
        FileSystem fs = mock(FileSystem.class);
        when(fs.listStatus(DIR)).thenThrow(new FileNotFoundException("already gone"));

        assertThat(FileSystemProbe.listStatus(fs, DIR, RateLimiter.create(1000.0))).isEmpty();
        verify(fs, times(2)).listStatus(DIR);
    }

    @Test
    void propagatesOrdinaryListingFailuresWithoutRetrying() throws IOException {
        FileSystem fs = mock(FileSystem.class);
        when(fs.listStatus(DIR)).thenThrow(new IOException("storage unavailable"));

        assertThatThrownBy(() -> FileSystemProbe.listStatus(fs, DIR, RateLimiter.create(1000.0)))
                .isInstanceOf(IOException.class)
                .hasMessage("storage unavailable");
        verify(fs).listStatus(DIR);
    }
}
