/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.lakehouse.writer;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.record.LogRecord;

import java.io.Closeable;
import java.io.IOException;

/**
 * The LakeWriter interface for writing records to a lake. It extends the Closeable interface to
 * ensure resources are released after use.
 *
 * @param <WriteResult> the type of the write result
 * @since 0.7
 */
@PublicEvolving
public interface LakeWriter<WriteResult> extends Closeable {
    /**
     * Writes a record to the lake.
     *
     * @param record the record to write
     * @throws IOException if an I/O error occurs
     */
    void write(LogRecord record) throws IOException;

    /**
     * Completes the writing process and returns the write result.
     *
     * @return the write result
     * @throws IOException if an I/O error occurs
     */
    WriteResult complete() throws IOException;
}
