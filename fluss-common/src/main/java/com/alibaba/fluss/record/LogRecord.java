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

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.row.InternalRow;

/**
 * A log record is a tuple consisting of a unique offset in the log, a changeType and a row.
 *
 * @since 0.1
 */
@PublicEvolving
public interface LogRecord {

    /**
     * The offset of this record in the log.
     *
     * @return the offset
     */
    long logOffset();

    /**
     * The commit timestamp of this record in the log.
     *
     * @return the timestamp
     */
    long timestamp();

    /**
     * Get the log record's {@link ChangeType}.
     *
     * @return the record's {@link ChangeType}.
     */
    ChangeType getChangeType();

    /**
     * Get the log record's row.
     *
     * @return the log record's row
     */
    InternalRow getRow();
}
