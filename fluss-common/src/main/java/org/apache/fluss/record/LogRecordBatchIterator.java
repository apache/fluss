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

package org.apache.fluss.record;

import org.apache.fluss.exception.CorruptMessageException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.utils.AbstractIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.EOFException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

/**
 * LogRecordBatchIterator is a subclass of AbstractIterator, which can iterate through instances of
 * LogRecordBatch.
 */
public class LogRecordBatchIterator<T extends LogRecordBatch> extends AbstractIterator<T> {

    private static final Logger LOG = LoggerFactory.getLogger(LogRecordBatchIterator.class);

    private LogInputStream<T> logInputStream;

    private @Nullable Long targetOffset;

    public LogRecordBatchIterator(LogInputStream<T> logInputStream) {
        this.logInputStream = logInputStream;
    }

    public LogRecordBatchIterator(LogInputStream<T> logInputStream, long targetOffset) {
        this.logInputStream = logInputStream;
        this.targetOffset = targetOffset;
    }

    private LogRecordBatchIterator() {}

    @Override
    protected T makeNext() {
        try {
            T batch = logInputStream.nextBatch();
            if (batch == null) {
                return allDone();
            }
            if (null == targetOffset || batch.lastLogOffset() >= targetOffset) {
                return batch;
            }
            return batch;
        } catch (EOFException e) {
            throw new CorruptMessageException(
                    "Unexpected EOF while attempting to read the next batch", e);
        } catch (IOException e) {
            throw new FlussRuntimeException(e);
        }
    }

    public LogRecordBatchIterator<T> filter(
            Filter recordBatchFilter, LogRecordBatch.ReadContext readContext) {
        return new FilteredLogRecordBatchIterator(this, recordBatchFilter, readContext);
    }

    private class FilteredLogRecordBatchIterator extends LogRecordBatchIterator<T> {

        private final Iterator<T> innerIter;

        private final Filter recordBatchFilter;

        private final LogRecordBatch.ReadContext readContext;

        private FilteredLogRecordBatchIterator(
                LogRecordBatchIterator<T> innerIter,
                Filter recordBatchFilter,
                LogRecordBatch.ReadContext readContext) {
            super();
            this.innerIter = innerIter;
            this.recordBatchFilter = recordBatchFilter;
            this.readContext = readContext;
        }

        @Override
        protected T makeNext() {
            while (innerIter.hasNext()) {
                T batch = innerIter.next();

                if (readContext == null) {
                    // If no ReadContext is provided, return all batches (backward compatibility)
                    return batch;
                }

                Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(readContext);
                if (!statisticsOpt.isPresent()) {
                    // If no statistics available, return the batch
                    return batch;
                }
                // Use the schema-aware test method if RecordBatchFilter is used
                boolean shouldIncludeBatch;
                try {
                    shouldIncludeBatch =
                            ((Filter) recordBatchFilter)
                                    .test(batch.getRecordCount(), statisticsOpt.get());
                } catch (Exception e) {
                    // If test method throws exception, log it and allow batch to pass through
                    LOG.warn(
                            "Exception occurred during record batch filtering, ignore testing.", e);
                    shouldIncludeBatch = true;
                }

                if (shouldIncludeBatch) {
                    return batch;
                }
            }
            return allDone();
        }
    }
}
