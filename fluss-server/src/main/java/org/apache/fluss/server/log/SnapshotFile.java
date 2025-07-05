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

package org.apache.fluss.server.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.apache.fluss.utils.FlussPaths.offsetFromFileName;

/** Snapshot file. */
public class SnapshotFile {
    private static final Logger LOG = LoggerFactory.getLogger(SnapshotFile.class);

    public final long offset;
    private final File file;

    public SnapshotFile(File file) {
        this(file, offsetFromFileName(file.getName()));
    }

    public SnapshotFile(File file, long offset) {
        this.file = file;
        this.offset = offset;
    }

    public boolean deleteIfExists() throws IOException {
        boolean deleted = Files.deleteIfExists(file.toPath());
        if (deleted) {
            LOG.info("Deleted writer state snapshot {}", file.getAbsolutePath());
        } else {
            LOG.info(
                    "Failed to delete writer snapshot {} because it does not exist.",
                    file.getAbsolutePath());
        }
        return deleted;
    }

    public File file() {
        return file;
    }

    @Override
    public String toString() {
        return "SnapshotFile(" + "offset=" + offset + ", file=" + file + ')';
    }
}
