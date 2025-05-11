/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.lakehouse.paimon.reader;

import com.alibaba.fluss.client.table.scanner.batch.BatchScanner;
import com.alibaba.fluss.utils.CloseableIterator;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** A scanner for reading paimon split. Most logic is copied from paimon. */
public class PaimonSnapshotScanner implements BatchScanner {

    private final TableRead tableRead;
    @Nullable private LazyRecordReader currentReader;

    public PaimonSnapshotScanner(TableRead tableRead, FileStoreSourceSplit fileStoreSourceSplit) {
        this.tableRead = tableRead;
        this.currentReader = new LazyRecordReader(fileStoreSourceSplit.split());
    }

    @Override
    @Nullable
    public CloseableIterator<com.alibaba.fluss.row.InternalRow> pollBatch(Duration timeout) {
        try {
            RecordReader.RecordIterator<InternalRow> nextBatch =
                    Objects.requireNonNull(currentReader).recordReader().readBatch();
            if (nextBatch == null) {
                return null;
            } else {
                return new PaimonRowIteratorWrapper(nextBatch);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() throws IOException {
        LazyRecordReader recordReader = currentReader;
        if (recordReader != null) {
            if (recordReader.lazyRecordReader != null) {
                recordReader.lazyRecordReader.close();
            }
            currentReader = null;
        }
    }

    private static class PaimonRowIteratorWrapper
            implements CloseableIterator<com.alibaba.fluss.row.InternalRow> {
        private final RecordReader.RecordIterator<InternalRow> recordBatch;
        private @Nullable InternalRow paimonRow;

        public PaimonRowIteratorWrapper(RecordReader.RecordIterator<InternalRow> recordBatch) {
            this.recordBatch = recordBatch;
        }

        @Override
        public boolean hasNext() {
            if (paimonRow != null) {
                return true;
            }
            try {
                paimonRow = recordBatch.next();
                return paimonRow != null;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public com.alibaba.fluss.row.InternalRow next() {
            PaimonRowWrapper wrapper = new PaimonRowWrapper(paimonRow);
            paimonRow = null;
            return wrapper;
        }

        @Override
        public void close() {
            recordBatch.releaseBatch();
        }
    }

    /** Lazy to create {@link RecordReader} to improve performance for limit. */
    private class LazyRecordReader {
        private final Split split;
        private RecordReader<InternalRow> lazyRecordReader;

        private LazyRecordReader(Split split) {
            this.split = split;
        }

        public RecordReader<InternalRow> recordReader() throws IOException {
            if (lazyRecordReader == null) {
                lazyRecordReader = tableRead.createReader(split);
            }
            return lazyRecordReader;
        }
    }
}
