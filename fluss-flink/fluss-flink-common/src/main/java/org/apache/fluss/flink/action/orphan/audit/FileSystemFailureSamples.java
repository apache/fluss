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

package org.apache.fluss.flink.action.orphan.audit;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.fs.FileSystemFailure;
import org.apache.fluss.fs.FsPath;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/** Keeps bounded first and latest exception samples for one scan subtask. */
@Internal
public final class FileSystemFailureSamples {

    private static final int MAX_GROUPS = 16;
    private final Map<String, Samples> groups = new LinkedHashMap<String, Samples>();
    private long unsampledFailures;

    /** Records a final failed operation after any path-disappearance retry. */
    public void record(String operation, FsPath path, IOException failure) {
        FileSystemFailure normalized =
                failure instanceof FileSystemFailure ? (FileSystemFailure) failure : null;
        FileSystemFailure.Kind kind =
                normalized == null ? FileSystemFailure.Kind.UNEXPECTED : normalized.kind();
        String code = normalized == null ? null : normalized.serviceCode();
        String key = path.toUri().getScheme() + "|" + operation + "|" + kind + "|" + code;
        Samples samples = groups.get(key);
        if (samples == null) {
            if (groups.size() == MAX_GROUPS) {
                unsampledFailures++;
                return;
            }
            samples = new Samples();
            groups.put(key, samples);
        }
        Sample sample = new Sample(operation, path, failure);
        if (samples.first == null) {
            samples.first = sample;
        }
        samples.latest = sample;
    }

    /** Emits the retained exception stacks with run and subtask identifiers. */
    public void emit(ResultAuditLogger audit, int subtask, int attempt) {
        for (Samples samples : groups.values()) {
            audit.filesystemFailureSample(
                    subtask,
                    attempt,
                    "first",
                    samples.first.operation,
                    samples.first.path,
                    samples.first.failure);
            if (samples.latest != samples.first) {
                audit.filesystemFailureSample(
                        subtask,
                        attempt,
                        "latest",
                        samples.latest.operation,
                        samples.latest.path,
                        samples.latest.failure);
            }
        }
        if (unsampledFailures > 0) {
            audit.filesystemFailureSamplesTruncated(subtask, attempt, unsampledFailures);
        }
    }

    private static final class Samples {
        private Sample first;
        private Sample latest;
    }

    private static final class Sample {
        private final String operation;
        private final FsPath path;
        private final IOException failure;

        private Sample(String operation, FsPath path, IOException failure) {
            this.operation = operation;
            this.path = path;
            this.failure = failure;
        }
    }
}
