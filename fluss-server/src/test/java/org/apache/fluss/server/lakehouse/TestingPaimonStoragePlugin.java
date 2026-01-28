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

package org.apache.fluss.server.lakehouse;

import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.CommitterInitContext;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.lakestorage.LakeCatalog;
import org.apache.fluss.lake.lakestorage.LakeSnapshotInfo;
import org.apache.fluss.lake.lakestorage.LakeStorage;
import org.apache.fluss.lake.lakestorage.LakeStoragePlugin;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecord;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.config.ConfigBuilder.key;

/** A plugin of paimon just for testing purpose. */
public class TestingPaimonStoragePlugin implements LakeStoragePlugin {

    public static final String IDENTIFIER = DataLakeFormat.PAIMON.toString();

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public LakeStorage createLakeStorage(Configuration configuration) {
        return new TestingPaimonLakeStorage();
    }

    /** Paimon implementation of LakeStorage for testing purpose. */
    public static class TestingPaimonLakeStorage implements LakeStorage {

        @Override
        public LakeTieringFactory<?, ?> createLakeTieringFactory() {
            return new TestingPaimonTieringFactory();
        }

        @Override
        public LakeCatalog createLakeCatalog() {
            return new TestingPaimonCatalog();
        }

        @Override
        public LakeSource<?> createLakeSource(TablePath tablePath) {
            throw new UnsupportedOperationException("Not implemented");
        }
    }

    /** Paimon implementation of LakeCatalog for testing purpose. */
    public static class TestingPaimonCatalog implements LakeCatalog {

        private final Map<TablePath, TableDescriptor> tableByPath = new HashMap<>();

        @Override
        public void createTable(
                TablePath tablePath, TableDescriptor tableDescriptor, Context context)
                throws TableAlreadyExistException {
            if (tableByPath.containsKey(tablePath)) {
                TableDescriptor existingTable = tableByPath.get(tablePath);
                if (!existingTable.equals(tableDescriptor)) {
                    throw new TableAlreadyExistException("Table " + tablePath + " already exists.");
                }
            }
            tableByPath.put(tablePath, tableDescriptor);
        }

        @Override
        public void alterTable(TablePath tablePath, List<TableChange> tableChanges, Context context)
                throws TableNotExistException {
            // do nothing
        }

        @Override
        public Optional<LakeSnapshotInfo> getLatestSnapshotInfo(
                TablePath tablePath, Context context) {
            return Optional.empty();
        }

        public TableDescriptor getTable(TablePath tablePath) {
            return tableByPath.get(tablePath);
        }
    }

    private static class TestingPaimonTieringFactory
            implements LakeTieringFactory<TestingPaimonWriteResult, TestPaimonCommittable> {

        @Override
        public LakeWriter<TestingPaimonWriteResult> createLakeWriter(
                WriterInitContext writerInitContext) {
            return new TestingPaimonWriter(writerInitContext.tableInfo());
        }

        @Override
        public SimpleVersionedSerializer<TestingPaimonWriteResult> getWriteResultSerializer() {
            return new TestingPaimonWriteResultSerializer();
        }

        @Override
        public LakeCommitter<TestingPaimonWriteResult, TestPaimonCommittable> createLakeCommitter(
                CommitterInitContext committerInitContext) throws IOException {
            return new TestingPaimonCommitter();
        }

        @Override
        public SimpleVersionedSerializer<TestPaimonCommittable> getCommittableSerializer() {
            return new SimpleVersionedSerializer<TestPaimonCommittable>() {
                @Override
                public int getVersion() {
                    return 0;
                }

                @Override
                public byte[] serialize(TestPaimonCommittable obj) throws IOException {
                    return new byte[0];
                }

                @Override
                public TestPaimonCommittable deserialize(int version, byte[] serialized)
                        throws IOException {
                    return new TestPaimonCommittable();
                }
            };
        }
    }

    private static class TestingPaimonWriter implements LakeWriter<TestingPaimonWriteResult> {

        static ConfigOption<Duration> writePauseOption =
                key("write-pause").durationType().noDefaultValue();

        private int writtenRecords = 0;
        private final Duration writePause;

        private TestingPaimonWriter(TableInfo tableInfo) {
            this.writePause = tableInfo.getCustomProperties().get(writePauseOption);
        }

        @Override
        public void write(LogRecord record) throws IOException {
            try {
                if (writePause != null) {
                    Thread.sleep(writePause.toMillis());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while pausing before write", e);
            }
            writtenRecords += 1;
        }

        @Override
        public TestingPaimonWriteResult complete() throws IOException {
            return new TestingPaimonWriteResult(writtenRecords);
        }

        @Override
        public void close() throws IOException {
            // do nothing
        }
    }

    private static class TestingPaimonWriteResult {
        private final int writtenRecords;

        public TestingPaimonWriteResult(int writtenRecords) {
            this.writtenRecords = writtenRecords;
        }
    }

    private static class TestingPaimonWriteResultSerializer
            implements SimpleVersionedSerializer<TestingPaimonWriteResult> {

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(TestingPaimonWriteResult result) throws IOException {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeInt(result.writtenRecords);
                return baos.toByteArray();
            }
        }

        @Override
        public TestingPaimonWriteResult deserialize(int version, byte[] serialized)
                throws IOException {
            try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                    ObjectInputStream ois = new ObjectInputStream(bais)) {
                return new TestingPaimonWriteResult(ois.readInt());
            }
        }
    }

    private static class TestPaimonCommittable {}

    private static class TestingPaimonCommitter
            implements LakeCommitter<TestingPaimonWriteResult, TestPaimonCommittable> {

        @Override
        public TestPaimonCommittable toCommittable(
                List<TestingPaimonWriteResult> testingPaimonWriteResults) throws IOException {
            return new TestPaimonCommittable();
        }

        @Override
        public long commit(
                TestPaimonCommittable committable, Map<String, String> snapshotProperties)
                throws IOException {
            // do nothing, and always return 1 as committed snapshot
            return 1;
        }

        @Override
        public void abort(TestPaimonCommittable committable) throws IOException {
            // do nothing
        }

        @Nullable
        @Override
        public CommittedLakeSnapshot getMissingLakeSnapshot(
                @Nullable Long latestLakeSnapshotIdOfFluss) throws IOException {
            return null;
        }

        @Override
        public void close() throws Exception {
            // do nothing
        }
    }
}
