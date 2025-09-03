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

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.lake.paimon.utils.PaimonRowAsFlussRow;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.IntStream;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toChangeType;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;

/** Record reader for paimon table. */
public class PaimonRecordReader implements RecordReader {

    protected PaimonRowAsFlussRecordIterator iterator;
    protected @Nullable int[][] project;
    protected @Nullable Predicate predicate;
    protected RowType paimonRowType;

    public PaimonRecordReader(
            FileStoreTable fileStoreTable,
            PaimonSplit split,
            @Nullable int[][] project,
            @Nullable Predicate predicate)
            throws IOException {
        ReadBuilder readBuilder = fileStoreTable.newReadBuilder();
        RowType paimonFullRowType = fileStoreTable.rowType();
        if (project != null) {
            readBuilder = applyProject(readBuilder, project, paimonFullRowType);
        }

        if (predicate != null) {
            readBuilder.withFilter(predicate);
        }

        TableRead tableRead = readBuilder.newRead().executeFilter();
        paimonRowType = readBuilder.readType();

        org.apache.paimon.reader.RecordReader<InternalRow> recordReader =
                tableRead.createReader(split.dataSplit());
        iterator =
                new PaimonRecordReader.PaimonRowAsFlussRecordIterator(
                        recordReader.toCloseableIterator(), paimonRowType);
    }

    @Override
    public CloseableIterator<LogRecord> read() throws IOException {
        return iterator;
    }

    private ReadBuilder applyProject(
            ReadBuilder readBuilder, int[][] projects, RowType paimonFullRowType) {
        int[] projectIds = Arrays.stream(projects).mapToInt(project -> project[0]).toArray();

        int offsetFieldPos = paimonFullRowType.getFieldIndex(OFFSET_COLUMN_NAME);
        int timestampFieldPos = paimonFullRowType.getFieldIndex(TIMESTAMP_COLUMN_NAME);

        int[] paimonProject =
                IntStream.concat(
                                IntStream.of(projectIds),
                                IntStream.of(offsetFieldPos, timestampFieldPos))
                        .toArray();

        return readBuilder.withProjection(paimonProject);
    }

    /** Iterator for paimon row as fluss record. */
    public static class PaimonRowAsFlussRecordIterator implements CloseableIterator<LogRecord> {

        private final org.apache.paimon.utils.CloseableIterator<InternalRow> paimonRowIterator;

        private final ProjectedRow projectedRow;
        private final PaimonRowAsFlussRow paimonRowAsFlussRow;

        private final int logOffsetColIndex;
        private final int timestampColIndex;

        public PaimonRowAsFlussRecordIterator(
                org.apache.paimon.utils.CloseableIterator<InternalRow> paimonRowIterator,
                RowType paimonRowType) {
            this.paimonRowIterator = paimonRowIterator;
            this.logOffsetColIndex = paimonRowType.getFieldIndex(OFFSET_COLUMN_NAME);
            this.timestampColIndex = paimonRowType.getFieldIndex(TIMESTAMP_COLUMN_NAME);

            int[] project = IntStream.range(0, paimonRowType.getFieldCount() - 2).toArray();
            projectedRow = ProjectedRow.from(project);
            paimonRowAsFlussRow = new PaimonRowAsFlussRow();
        }

        @Override
        public void close() {
            try {
                paimonRowIterator.close();
            } catch (Exception e) {
                throw new RuntimeException("Fail to close iterator.", e);
            }
        }

        @Override
        public boolean hasNext() {
            return paimonRowIterator.hasNext();
        }

        @Override
        public LogRecord next() {
            InternalRow paimonRow = paimonRowIterator.next();
            ChangeType changeType = toChangeType(paimonRow.getRowKind());
            long offset = paimonRow.getLong(logOffsetColIndex);
            long timestamp = paimonRow.getTimestamp(timestampColIndex, 6).getMillisecond();

            return new GenericRecord(
                    offset,
                    timestamp,
                    changeType,
                    projectedRow.replaceRow(paimonRowAsFlussRow.replaceRow(paimonRow)));
        }
    }
}
