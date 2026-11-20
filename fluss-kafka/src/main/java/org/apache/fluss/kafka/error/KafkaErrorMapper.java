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

package org.apache.fluss.kafka.error;

import org.apache.fluss.annotation.Internal;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/** Maps failures from the compatibility layer to version-aware Kafka responses. */
@Internal
public final class KafkaErrorMapper {

    /** Converts a failure to the error response defined by the parsed Kafka request. */
    public AbstractResponse toResponse(AbstractRequest request, Throwable failure) {
        return request.getErrorResponse(unwrap(failure));
    }

    /** Maps native Fluss errors to their Kafka protocol equivalents. */
    public static Errors toKafkaError(org.apache.fluss.rpc.protocol.Errors error) {
        switch (error) {
            case NONE:
                return Errors.NONE;
            case TABLE_NOT_EXIST:
            case DATABASE_NOT_EXIST:
            case UNKNOWN_TABLE_OR_BUCKET_EXCEPTION:
                return Errors.UNKNOWN_TOPIC_OR_PARTITION;
            case NOT_LEADER_OR_FOLLOWER:
                return Errors.NOT_LEADER_OR_FOLLOWER;
            case LEADER_NOT_AVAILABLE_EXCEPTION:
                return Errors.LEADER_NOT_AVAILABLE;
            case RECORD_TOO_LARGE_EXCEPTION:
                return Errors.MESSAGE_TOO_LARGE;
            case CORRUPT_MESSAGE:
            case CORRUPT_RECORD_EXCEPTION:
                return Errors.CORRUPT_MESSAGE;
            case INVALID_REQUIRED_ACKS:
                return Errors.INVALID_REQUIRED_ACKS;
            case REQUEST_TIME_OUT:
                return Errors.REQUEST_TIMED_OUT;
            case NOT_ENOUGH_REPLICAS_EXCEPTION:
                return Errors.NOT_ENOUGH_REPLICAS;
            case NOT_ENOUGH_REPLICAS_AFTER_APPEND_EXCEPTION:
                return Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND;
            case AUTHORIZATION_EXCEPTION:
                return Errors.TOPIC_AUTHORIZATION_FAILED;
            case LOG_STORAGE_EXCEPTION:
            case STORAGE_EXCEPTION:
            case DISK_WRITE_LOCKED:
                return Errors.KAFKA_STORAGE_ERROR;
            default:
                return Errors.UNKNOWN_SERVER_ERROR;
        }
    }

    public static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
