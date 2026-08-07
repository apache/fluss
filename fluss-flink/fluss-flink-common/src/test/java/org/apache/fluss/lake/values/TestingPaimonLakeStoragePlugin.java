/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.values;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.lake.lakestorage.LakeCatalog;
import org.apache.fluss.lake.lakestorage.LakeStorage;
import org.apache.fluss.lake.lakestorage.LakeStoragePlugin;
import org.apache.fluss.lake.lakestorage.LakeTableLookuper;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.row.encode.ValueEncoder;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Test-only Paimon lake storage plugin used to construct Flink table sources. */
public class TestingPaimonLakeStoragePlugin implements LakeStoragePlugin {

    private static final LookupFunction EMPTY_LOOKUP = (key, context) -> null;

    private static volatile LookupFunction lookupFunction = EMPTY_LOOKUP;

    /** Configures the point lookup behavior used by newly-created and existing test lookupers. */
    public static void setLookupFunction(LookupFunction lookupFunction) {
        TestingPaimonLakeStoragePlugin.lookupFunction =
                checkNotNull(lookupFunction, "lookupFunction must not be null.");
    }

    /** Resets point lookups to return no row. */
    public static void resetLookupFunction() {
        lookupFunction = EMPTY_LOOKUP;
    }

    @Override
    public String identifier() {
        return DataLakeFormat.PAIMON.toString();
    }

    @Override
    public LakeStorage createLakeStorage(Configuration configuration) {
        return new TestingPaimonLakeStorage();
    }

    private static class TestingPaimonLakeStorage implements LakeStorage {
        @Override
        public LakeTieringFactory<?, ?> createLakeTieringFactory() {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public LakeCatalog createLakeCatalog() {
            return new TestingPaimonLakeCatalog();
        }

        @Override
        public LakeSource<?> createLakeSource(TablePath tablePath) {
            return new TestingPaimonLakeSource();
        }

        @Override
        public LakeTableLookuper createLakeTableLookuper(
                TablePath tablePath, LookuperContext context) {
            return new TestingLakeTableLookuper(context);
        }
    }

    private static class TestingLakeTableLookuper implements LakeTableLookuper {

        private final LakeStorage.LookuperContext lookuperContext;

        private TestingLakeTableLookuper(LakeStorage.LookuperContext lookuperContext) {
            this.lookuperContext = lookuperContext;
        }

        @Override
        public @Nullable byte[] lookup(byte[] key, LookupContext context) throws Exception {
            InternalRow row = lookupFunction.lookup(key, context);
            if (row == null) {
                return null;
            }

            try (RowEncoder rowEncoder =
                    RowEncoder.create(
                            lookuperContext.tableConfig().getKvFormat(), context.valueRowType())) {
                InternalRow.FieldGetter[] fieldGetters =
                        InternalRow.createFieldGetters(context.valueRowType());
                rowEncoder.startNewRow();
                for (int i = 0; i < fieldGetters.length; i++) {
                    rowEncoder.encodeField(i, fieldGetters[i].getFieldOrNull(row));
                }
                BinaryRow binaryRow = rowEncoder.finishRow();
                return ValueEncoder.encodeValue(context.schemaId(), binaryRow);
            }
        }

        @Override
        public void close() {}
    }

    private static class TestingPaimonLakeCatalog implements LakeCatalog {
        @Override
        public void createTable(
                TablePath tablePath, TableDescriptor tableDescriptor, Context context)
                throws TableAlreadyExistException {}

        @Override
        public void alterTable(TablePath tablePath, List<TableChange> tableChanges, Context context)
                throws TableNotExistException {}
    }

    private static class TestingPaimonLakeSource implements LakeSource<LakeSplit> {
        @Override
        public void withProject(int[][] project) {}

        @Override
        public void withLimit(int limit) {}

        @Override
        public FilterPushDownResult withFilters(List<Predicate> predicates) {
            return FilterPushDownResult.of(predicates, Collections.emptyList());
        }

        @Override
        public Planner<LakeSplit> createPlanner(PlannerContext context) throws IOException {
            return Collections::emptyList;
        }

        @Override
        public RecordReader createRecordReader(ReaderContext<LakeSplit> context) {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public SimpleVersionedSerializer<LakeSplit> getSplitSerializer() {
            throw new UnsupportedOperationException("Not implemented.");
        }
    }

    /** Test callback for a lake point lookup. */
    @FunctionalInterface
    public interface LookupFunction {

        /** Returns the row for the encoded key, or null when the key is absent. */
        @Nullable
        InternalRow lookup(byte[] key, LakeTableLookuper.LookupContext context) throws Exception;
    }
}
