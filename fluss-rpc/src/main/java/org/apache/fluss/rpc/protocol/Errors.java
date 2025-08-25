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

package org.apache.fluss.rpc.protocol;

import org.apache.fluss.exception.ApiException;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.exception.CorruptMessageException;
import org.apache.fluss.exception.CorruptRecordException;
import org.apache.fluss.exception.DatabaseAlreadyExistException;
import org.apache.fluss.exception.DatabaseNotEmptyException;
import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.fluss.exception.DuplicateSequenceException;
import org.apache.fluss.exception.FencedLeaderEpochException;
import org.apache.fluss.exception.FencedTieringEpochException;
import org.apache.fluss.exception.InvalidColumnProjectionException;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.exception.InvalidCoordinatorException;
import org.apache.fluss.exception.InvalidDatabaseException;
import org.apache.fluss.exception.InvalidPartitionException;
import org.apache.fluss.exception.InvalidReplicationFactorException;
import org.apache.fluss.exception.InvalidRequiredAcksException;
import org.apache.fluss.exception.InvalidServerRackInfoException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.InvalidTargetColumnException;
import org.apache.fluss.exception.InvalidTimestampException;
import org.apache.fluss.exception.InvalidUpdateVersionException;
import org.apache.fluss.exception.KvSnapshotNotExistException;
import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.exception.LakeStorageNotConfiguredException;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.exception.LeaderNotAvailableException;
import org.apache.fluss.exception.LogOffsetOutOfRangeException;
import org.apache.fluss.exception.LogStorageException;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.exception.NonPrimaryKeyTableException;
import org.apache.fluss.exception.NotEnoughReplicasAfterAppendException;
import org.apache.fluss.exception.NotEnoughReplicasException;
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.exception.OperationNotAttemptedException;
import org.apache.fluss.exception.OutOfOrderSequenceException;
import org.apache.fluss.exception.PartitionAlreadyExistsException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.RecordTooLargeException;
import org.apache.fluss.exception.RetriableAuthenticationException;
import org.apache.fluss.exception.SchemaNotExistException;
import org.apache.fluss.exception.SecurityDisabledException;
import org.apache.fluss.exception.SecurityTokenException;
import org.apache.fluss.exception.StorageException;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.exception.TableNotPartitionedException;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.exception.TooManyBucketsException;
import org.apache.fluss.exception.TooManyPartitionsException;
import org.apache.fluss.exception.UnknownServerException;
import org.apache.fluss.exception.UnknownTableOrBucketException;
import org.apache.fluss.exception.UnknownWriterIdException;
import org.apache.fluss.exception.UnsupportedVersionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/**
 * This class contains all the client-server errors--those errors that must be sent from the server
 * to the client. These are thus part of the protocol. The names can be changed but the error code
 * cannot.
 *
 * <p>Do not add exceptions that occur only on the client or only on the server here.
 */
public enum Errors {
    UNKNOWN_SERVER_ERROR(
            -1,
            "The server experienced an unexpected error when processing the request.",
            UnknownServerException::new),
    NONE(0, null, message -> null),
    NETWORK_EXCEPTION(
            1, "The server disconnected before a response was received.", NetworkException::new),
    UNSUPPORTED_VERSION(
            2, "The version of API is not supported.", UnsupportedVersionException::new),
    CORRUPT_MESSAGE(
            3,
            "This message has failed its CRC checksum, exceeds the valid size, has a null key for a primary key table, or is otherwise corrupt.",
            CorruptMessageException::new),
    DATABASE_NOT_EXIST(4, "The database does not exist.", DatabaseNotExistException::new),
    DATABASE_NOT_EMPTY(5, "The database is not empty.", DatabaseNotEmptyException::new),
    DATABASE_ALREADY_EXIST(6, "The database already exists.", DatabaseAlreadyExistException::new),
    TABLE_NOT_EXIST(7, "The table does not exist.", TableNotExistException::new),
    TABLE_ALREADY_EXIST(8, "The table already exists.", TableAlreadyExistException::new),
    SCHEMA_NOT_EXIST(9, "The schema does not exist.", SchemaNotExistException::new),
    LOG_STORAGE_EXCEPTION(
            10, "Exception occur while storage data for log in server.", LogStorageException::new),
    KV_STORAGE_EXCEPTION(
            11, "Exception occur while storage data for kv in server.", KvStorageException::new),
    NOT_LEADER_OR_FOLLOWER(12, "Not leader or follower.", NotLeaderOrFollowerException::new),
    RECORD_TOO_LARGE_EXCEPTION(13, "The record is too large.", RecordTooLargeException::new),
    CORRUPT_RECORD_EXCEPTION(14, "The record is corrupt.", CorruptRecordException::new),
    INVALID_TABLE_EXCEPTION(
            15,
            "The client has attempted to perform an operation on an invalid table.",
            InvalidTableException::new),
    INVALID_DATABASE_EXCEPTION(
            16,
            "The client has attempted to perform an operation on an invalid database.",
            InvalidDatabaseException::new),
    INVALID_REPLICATION_FACTOR(
            17,
            "The replication factor is larger then the number of available tablet servers.",
            InvalidReplicationFactorException::new),
    INVALID_REQUIRED_ACKS(
            18,
            "Produce request specified an invalid value for required acks.",
            InvalidRequiredAcksException::new),
    LOG_OFFSET_OUT_OF_RANGE_EXCEPTION(
            19, "The log offset is out of range.", LogOffsetOutOfRangeException::new),
    NON_PRIMARY_KEY_TABLE_EXCEPTION(
            20, "The table is not primary key table.", NonPrimaryKeyTableException::new),
    UNKNOWN_TABLE_OR_BUCKET_EXCEPTION(
            21, "The table or bucket does not exist.", UnknownTableOrBucketException::new),
    INVALID_UPDATE_VERSION_EXCEPTION(
            22, "The update version is invalid.", InvalidUpdateVersionException::new),
    INVALID_COORDINATOR_EXCEPTION(
            23, "The coordinator is invalid.", InvalidCoordinatorException::new),
    FENCED_LEADER_EPOCH_EXCEPTION(
            24, "The leader epoch is invalid.", FencedLeaderEpochException::new),
    REQUEST_TIME_OUT(25, "The request time out.", TimeoutException::new),
    STORAGE_EXCEPTION(26, "The general storage exception.", StorageException::new),
    OPERATION_NOT_ATTEMPTED_EXCEPTION(
            27,
            "The server did not attempt to execute this operation.",
            OperationNotAttemptedException::new),
    NOT_ENOUGH_REPLICAS_AFTER_APPEND_EXCEPTION(
            28,
            "Records are written to the server already, but to fewer in-sync replicas than required.",
            NotEnoughReplicasAfterAppendException::new),
    NOT_ENOUGH_REPLICAS_EXCEPTION(
            29,
            "Messages are rejected since there are fewer in-sync replicas than required.",
            NotEnoughReplicasException::new),
    SECURITY_TOKEN_EXCEPTION(
            30, "Get file access security token exception.", SecurityTokenException::new),
    OUT_OF_ORDER_SEQUENCE_EXCEPTION(
            31,
            "The tablet server received an out of order sequence batch.",
            OutOfOrderSequenceException::new),
    DUPLICATE_SEQUENCE_EXCEPTION(
            32,
            "The tablet server received a duplicate sequence batch.",
            DuplicateSequenceException::new),
    UNKNOWN_WRITER_ID_EXCEPTION(
            33,
            "This exception is raised by the tablet server if it could not locate the writer metadata.",
            UnknownWriterIdException::new),
    INVALID_COLUMN_PROJECTION(
            34,
            "The requested column projection is invalid.",
            InvalidColumnProjectionException::new),
    INVALID_TARGET_COLUMN(
            35,
            "The requested target column to write is invalid.",
            InvalidTargetColumnException::new),
    PARTITION_NOT_EXISTS(36, "The partition does not exist.", PartitionNotExistException::new),
    TABLE_NOT_PARTITIONED_EXCEPTION(
            37, "The table is not partitioned.", TableNotPartitionedException::new),
    INVALID_TIMESTAMP_EXCEPTION(38, "The timestamp is invalid.", InvalidTimestampException::new),
    INVALID_CONFIG_EXCEPTION(39, "The config is invalid.", InvalidConfigException::new),
    LAKE_STORAGE_NOT_CONFIGURED_EXCEPTION(
            40, "The lake storage is not configured.", LakeStorageNotConfiguredException::new),
    KV_SNAPSHOT_NOT_EXIST(41, "The kv snapshot is not exist.", KvSnapshotNotExistException::new),
    PARTITION_ALREADY_EXISTS(
            42, "The partition already exists.", PartitionAlreadyExistsException::new),
    PARTITION_SPEC_INVALID_EXCEPTION(
            43, "The partition spec is invalid.", InvalidPartitionException::new),
    LEADER_NOT_AVAILABLE_EXCEPTION(
            44,
            "There is no currently available leader for the given partition.",
            LeaderNotAvailableException::new),
    PARTITION_MAX_NUM_EXCEPTION(
            45, "Exceed the maximum number of partitions.", TooManyPartitionsException::new),
    AUTHENTICATE_EXCEPTION(46, "Authentication failed.", AuthenticationException::new),
    SECURITY_DISABLED_EXCEPTION(47, "Security is disabled.", SecurityDisabledException::new),
    AUTHORIZATION_EXCEPTION(48, "Authorization failed", AuthorizationException::new),
    BUCKET_MAX_NUM_EXCEPTION(
            49, "Exceed the maximum number of buckets", TooManyBucketsException::new),
    FENCED_TIERING_EPOCH_EXCEPTION(
            50, "The tiering epoch is invalid.", FencedTieringEpochException::new),
    RETRIABLE_AUTHENTICATE_EXCEPTION(
            51,
            "Authentication failed with retriable exception. ",
            RetriableAuthenticationException::new),
    INVALID_SERVER_RACK_INFO_EXCEPTION(
            52, "The server rack info is invalid.", InvalidServerRackInfoException::new),
    LAKE_SNAPSHOT_NOT_EXIST(
            53, "The lake snapshot is not exist.", LakeTableSnapshotNotExistException::new);

    private static final Logger LOG = LoggerFactory.getLogger(Errors.class);

    private static final Map<Class<?>, Errors> CLASS_TO_ERROR = new HashMap<>();
    private static final Map<Integer, Errors> CODE_TO_ERROR = new HashMap<>();

    static {
        for (Errors error : Errors.values()) {
            if (CODE_TO_ERROR.put(error.code(), error) != null) {
                throw new ExceptionInInitializerError(
                        "Code " + error.code() + " for error " + error + " has already been used");
            }

            if (error.exception != null) {
                CLASS_TO_ERROR.put(error.exception.getClass(), error);
            }
        }
    }

    private final int code;
    private final Function<String, ApiException> builder;
    private final ApiException exception;

    Errors(int code, String defaultExceptionString, Function<String, ApiException> builder) {
        this.code = code;
        this.builder = builder;
        this.exception = builder.apply(defaultExceptionString);
    }

    /** An instance of the exception. */
    public ApiException exception() {
        return this.exception;
    }

    /** Create an instance of the ApiException that contains the given error message. */
    public ApiException exception(String message) {
        if (message == null) {
            // If no error message was specified, return an exception with the default error
            // message.
            return exception;
        }
        // Return an exception with the given error message.
        return builder.apply(message);
    }

    /** Returns the class name of the exception or null if this is {@code Errors.NONE}. */
    public String exceptionName() {
        return exception == null ? null : exception.getClass().getName();
    }

    /** The error code for the exception. */
    public int code() {
        return this.code;
    }

    /** Throw the exception corresponding to this error if there is one. */
    public void maybeThrow() {
        if (exception != null) {
            throw this.exception;
        }
    }

    /**
     * Get a friendly description of the error (if one is available), returns null for {@link
     * #NONE}.
     */
    @Nullable
    public String message() {
        if (exception != null) {
            return exception.getMessage();
        }
        return null;
    }

    public ApiError toApiError() {
        return new ApiError(this, message());
    }

    /** Throw the exception if there is one. */
    public static Errors forCode(int code) {
        Errors error = CODE_TO_ERROR.get(code);
        if (error != null) {
            return error;
        } else {
            LOG.warn("Unexpected error code: {}.", code);
            return UNKNOWN_SERVER_ERROR;
        }
    }

    /**
     * Return the error instance associated with this exception or any of its superclasses (or
     * UNKNOWN if there is none). If there are multiple matches in the class hierarchy, the first
     * match starting from the bottom is used.
     */
    public static Errors forException(Throwable t) {
        Throwable cause = maybeUnwrapException(t);
        Class<?> clazz = cause.getClass();
        while (clazz != null) {
            Errors error = CLASS_TO_ERROR.get(clazz);
            if (error != null) {
                return error;
            }
            clazz = clazz.getSuperclass();
        }
        return UNKNOWN_SERVER_ERROR;
    }

    /**
     * Check if a Throwable is a commonly wrapped exception type (e.g. `CompletionException`) and
     * return the cause if so. This is useful to handle cases where exceptions may be raised from a
     * future or a completion stage (as might be the case for requests sent to the RPC Gateway).
     *
     * @param t The Throwable to check
     * @return The throwable itself or its cause if it is an instance of a commonly wrapped
     *     exception type
     */
    public static Throwable maybeUnwrapException(Throwable t) {
        if (t instanceof CompletionException || t instanceof ExecutionException) {
            return t.getCause();
        } else {
            return t;
        }
    }
}
