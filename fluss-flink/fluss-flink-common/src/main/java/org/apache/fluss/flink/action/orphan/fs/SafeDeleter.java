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
import org.apache.fluss.flink.action.orphan.audit.AuditLogger;
import org.apache.fluss.flink.action.orphan.rule.Decision;
import org.apache.fluss.flink.action.orphan.rule.RuleId;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.RateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Sole entry point for filesystem deletion within the orphan cleanup package.
 *
 * <p>Only two operations are exposed:
 *
 * <ul>
 *   <li>{@link #deleteFile} - delete a single file (never recursive).
 *   <li>{@link #deleteEmptyDir} - delete a directory only if it is currently empty.
 * </ul>
 *
 * <p>By design there is no recursive-delete API; any caller that needs deletion under {@code
 * fluss-flink-common/.../action/orphan/} should go through this class. The single-entry-point
 * invariant is currently enforced only by convention — there is no Checkstyle rule guarding it.
 */
@Internal
public final class SafeDeleter {

    private static final Logger LOG = LoggerFactory.getLogger(SafeDeleter.class);

    private final FileSystem fs;
    private final boolean dryRun;
    private final AuditLogger audit;
    private final RateLimiter remoteFsOpRateLimiter;

    public SafeDeleter(
            FileSystem fs, boolean dryRun, AuditLogger audit, RateLimiter remoteFsOpRateLimiter) {
        this.fs = fs;
        this.dryRun = dryRun;
        this.audit = audit;
        this.remoteFsOpRateLimiter = remoteFsOpRateLimiter;
    }

    /**
     * Delete a single file.
     *
     * @return {@code true} if the file was actually deleted (or recorded as would-be-deleted under
     *     {@code dryRun}); {@code false} if {@link FileSystem#delete} returned {@code false}
     *     (deletion silently failed — e.g. permissions, transient remote-store error). Callers
     *     should track {@code false} returns as delete failures in their run summary.
     */
    public boolean deleteFile(FsPath file, Decision decision, RuleId ruleId) {
        checkArgument(
                decision == Decision.DELETE,
                "deleteFile must only be called for Decision.DELETE, got %s",
                decision);
        if (dryRun) {
            audit.logWouldDelete(file, ruleId);
            return true;
        }
        remoteFsOpRateLimiter.acquire();
        try {
            boolean ok = fs.delete(file, false);
            audit.logDeleted(file, ruleId, ok);
            return ok;
        } catch (IOException e) {
            LOG.warn("Failed to delete file: {}", file, e);
            audit.logDeleted(file, ruleId, false);
            return false;
        }
    }

    /**
     * Delete a directory only if it is currently empty.
     *
     * @return {@code true} if the directory was actually deleted (or recorded as would-be-deleted
     *     under {@code dryRun}); {@code false} if the directory was non-empty / unreadable, or if
     *     {@link FileSystem#delete} returned {@code false}. Callers should not increment a "deleted
     *     directory" counter when this returns {@code false}.
     */
    public boolean deleteEmptyDir(FsPath dir) {
        FileStatus[] children = listChildrenSilently(dir);
        if (children == null || children.length > 0) {
            return false;
        }
        if (dryRun) {
            audit.logWouldDeleteDir(dir);
            return true;
        }
        remoteFsOpRateLimiter.acquire();
        try {
            boolean ok = fs.delete(dir, false);
            if (ok) {
                audit.logDirDeleted(dir);
            }
            return ok;
        } catch (IOException e) {
            LOG.warn("Failed to delete empty directory: {}", dir, e);
            return false;
        }
    }

    private FileStatus[] listChildrenSilently(FsPath dir) {
        try {
            remoteFsOpRateLimiter.acquire();
            return fs.listStatus(dir);
        } catch (IOException ignored) {
            return null;
        }
    }
}
