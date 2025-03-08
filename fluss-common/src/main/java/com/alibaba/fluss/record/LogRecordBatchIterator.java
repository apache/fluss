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

package com.alibaba.fluss.record;

import com.alibaba.fluss.exception.CorruptMessageException;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.utils.AbstractIterator;

import java.io.EOFException;
import java.io.IOException;

/**
 * LogRecordBatchIterator is a subclass of AbstractIterator, which can iterate through instances of
 * LogRecordBatch.
 */
public class LogRecordBatchIterator<T extends LogRecordBatch> extends AbstractIterator<T> {

    private final LogInputStream<T> logInputStream;

    public LogRecordBatchIterator(LogInputStream<T> logInputStream) {
        this.logInputStream = logInputStream;
    }

    @Override
    protected T makeNext() {
        try {
            T batch = logInputStream.nextBatch();
            if (batch == null) {
                return allDone();
            }
            return batch;
        } catch (EOFException e) {
            throw new CorruptMessageException(
                    "Unexpected EOF while attempting to read the next batch", e);
        } catch (IOException e) {
            throw new FlussRuntimeException(e);
        }
    }
}
