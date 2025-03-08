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

package com.alibaba.fluss.exception;

import com.alibaba.fluss.annotation.PublicEvolving;

/**
 * This exception indicates a record has failed its internal CRC check, this generally indicates
 * network or disk corruption.
 *
 * @since 0.1
 */
@PublicEvolving
public class CorruptRecordException extends RetriableException {

    private static final long serialVersionUID = 1L;

    public CorruptRecordException() {
        super("This message has failed its CRC checksum, exceeds the valid size.");
    }

    public CorruptRecordException(String message) {
        super(message);
    }

    public CorruptRecordException(Throwable cause) {
        super(cause);
    }

    public CorruptRecordException(String message, Throwable cause) {
        super(message, cause);
    }
}
