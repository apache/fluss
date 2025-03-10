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
 * This exception is thrown if the producer cannot allocate memory for a record within max.block.ms
 * due to the buffer being too full.
 *
 * @since 0.1
 */
@PublicEvolving
public class BufferExhaustedException extends TimeoutException {

    private static final long serialVersionUID = 1L;

    public BufferExhaustedException(String message) {
        super(message);
    }
}
