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

import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.FlussPaths;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.stream.Stream;

import static org.apache.fluss.utils.FlussPaths.LOG_TABLET_DIR_PREFIX;
import static org.apache.fluss.utils.FlussPaths.WRITER_SNAPSHOT_FILE_SUFFIX;

/** Test utils for log. */
public class LogTestUtils {
    public static LogSegment createSegment(long offset, File dataDir, int indexIntervalBytes)
            throws IOException {
        FileLogRecords fileLogRecords = FileLogRecords.open(FlussPaths.logFile(dataDir, offset));
        LazyIndex<OffsetIndex> idx =
                LazyIndex.forOffset(FlussPaths.offsetIndexFile(dataDir, offset), offset, 1000);
        LazyIndex<TimeIndex> timeIdx =
                LazyIndex.forTime(FlussPaths.timeIndexFile(dataDir, offset), offset, 1500);
        return new LogSegment(
                LogFormat.ARROW, fileLogRecords, idx, timeIdx, offset, indexIntervalBytes);
    }

    public static void writeNonsenseToFile(File fileName, long position, int size)
            throws IOException {
        Random random = new Random();
        RandomAccessFile outputStream = new RandomAccessFile(fileName, "rw");
        outputStream.seek(position);
        try {
            for (int i = 0; i < size; i++) {
                outputStream.write(random.nextInt(255));
            }
        } finally {
            outputStream.close();
        }
    }

    /**
     * Create a random log directory in the format
     * /{database}/{table_name}-{table_id}/log-{bucket_id} used for Fluss bucket logs. It is the
     * responsibility of the caller to set up a shutdown hook for deletion of the directory.
     */
    public static File makeRandomLogTabletDir(
            File dataDir, String dbName, long tableId, String tableName) throws IOException {
        Path dbDir = Paths.get(dataDir.getAbsolutePath(), dbName);
        if (!Files.exists(dbDir)) {
            FileUtils.createDirectory(dataDir, dbName);
        }

        String tableDirName = tableName + "-" + tableId;
        Path tableDir = Paths.get(dbDir.toString(), tableDirName);
        if (!Files.exists(tableDir)) {
            FileUtils.createDirectory(dbDir.toFile(), tableDirName);
        }

        Random random = new Random(10L);
        int attempts = 1000;
        File f =
                Stream.generate(
                                () ->
                                        new File(
                                                tableDir.toFile(),
                                                LOG_TABLET_DIR_PREFIX + random.nextInt(1000000)))
                        .limit(attempts)
                        .filter(File::mkdir)
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "Failed to create directory after "
                                                        + attempts
                                                        + " attempts"));
        f.deleteOnExit();
        return f;
    }

    public static void deleteWriterSnapshotFiles(File logDir) throws IOException {
        File[] files = logDir.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isFile() && f.getName().endsWith(WRITER_SNAPSHOT_FILE_SUFFIX)) {
                    FileUtils.deleteFileOrDirectory(f);
                }
            }
        }
    }
}
