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
import com.alibaba.fluss.config.ConfigOptions;

/**
 * Number of in-sync replicas for the bucket is lower than {@link
 * ConfigOptions#LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER}. This exception is raised when the low ISR
 * size is discovered after the message was already appended to the log. Producer retries will cause
 * duplicates.
 *
 * @since 0.1
 */
@PublicEvolving
public class NotEnoughReplicasAfterAppendException extends RetriableException {
    public NotEnoughReplicasAfterAppendException(String message) {
        super(message);
    }
}
