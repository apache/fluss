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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.RateLimiter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;

/** Bounded filesystem probes that distinguish a disappeared path from an I/O failure. */
@Internal
public final class FileSystemProbe {

    private FileSystemProbe() {}

    public static Optional<FileStatus[]> listStatus(
            FileSystem fs, FsPath path, RateLimiter rateLimiter) throws IOException {
        try {
            return Optional.of(listOnce(fs, path, rateLimiter));
        } catch (FileNotFoundException firstNotFound) {
            try {
                return Optional.of(listOnce(fs, path, rateLimiter));
            } catch (FileNotFoundException confirmedNotFound) {
                return Optional.empty();
            }
        }
    }

    public static Optional<FileStatus> getFileStatus(
            FileSystem fs, FsPath path, RateLimiter rateLimiter) throws IOException {
        try {
            return Optional.of(getFileStatusOnce(fs, path, rateLimiter));
        } catch (FileNotFoundException firstNotFound) {
            try {
                return Optional.of(getFileStatusOnce(fs, path, rateLimiter));
            } catch (FileNotFoundException confirmedNotFound) {
                return Optional.empty();
            }
        }
    }

    private static FileStatus[] listOnce(FileSystem fs, FsPath path, RateLimiter rateLimiter)
            throws IOException {
        rateLimiter.acquire();
        FileStatus[] listing = fs.listStatus(path);
        if (listing == null) {
            // Some filesystems return null for a missing path as well as a failed listing.
            // Only an explicit NOT_FOUND from stat may enter the bounded absence retry.
            getFileStatusOnce(fs, path, rateLimiter);
            throw new IOException("Filesystem returned null while listing " + path);
        }
        return listing;
    }

    private static FileStatus getFileStatusOnce(FileSystem fs, FsPath path, RateLimiter rateLimiter)
            throws IOException {
        rateLimiter.acquire();
        return fs.getFileStatus(path);
    }
}
