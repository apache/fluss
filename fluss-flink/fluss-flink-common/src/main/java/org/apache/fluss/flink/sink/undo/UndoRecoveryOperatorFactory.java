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

package org.apache.fluss.flink.sink.undo;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.RowType;

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for creating {@link UndoRecoveryOperator} instances.
 *
 * <p>This factory is responsible for creating UndoRecoveryOperator instances with the provided
 * configuration. It implements {@link OneInputStreamOperatorFactory} to integrate with Flink's
 * operator creation mechanism.
 *
 * <p><b>Operator Chaining:</b> The factory sets {@link ChainingStrategy#ALWAYS} to enable operator
 * chaining. This allows the UndoRecoveryOperator to be chained with downstream operators (like the
 * SinkWriter) for better performance by reducing serialization overhead and network communication.
 *
 * <p><b>ProducerOffsetReporter:</b> For UNDO, the factory's shared {@link
 * ProducerOffsetReporterHolder} bridges the downstream SinkWriter and the operator. NO_OP keeps the
 * same operator topology but does not attach or register this callback.
 *
 * @param <IN> The type of input elements
 * @see UndoRecoveryOperator
 * @see ProducerOffsetReporter
 */
@Internal
public class UndoRecoveryOperatorFactory<IN> extends AbstractStreamOperatorFactory<IN>
        implements OneInputStreamOperatorFactory<IN, IN> {

    private static final long serialVersionUID = 1L;

    // ==================== Configuration Fields ====================

    /** The table path for the Fluss table. */
    private final TablePath tablePath;

    /** The Fluss configuration. */
    private final Configuration flussConfig;

    /** The row type of the table. */
    private final RowType tableRowType;

    /** Target column indexes for partial update (null for full row). */
    @Nullable private final int[] targetColumnIndexes;

    /** The number of buckets in the table. */
    private final int numBuckets;

    /** Whether the table is partitioned. */
    private final boolean isPartitioned;

    /**
     * The producer ID used for producer offset snapshot management.
     *
     * <p>This is used by {@link RecoveryOffsetManager} to register and retrieve producer offsets
     * for pre-checkpoint failure recovery. If null, UNDO uses the Flink job ID while NO_OP performs
     * no producer-offset operation.
     */
    @Nullable private final String producerId;

    private final RecoveryAction recoveryAction;

    /** The polling interval in milliseconds for producer offsets synchronization. */
    private final long producerOffsetsPollIntervalMs;

    /** The maximum total time in milliseconds to poll for producer offsets before giving up. */
    private final long maxPollTimeoutMs;

    // ==================== Runtime Fields ====================

    /**
     * The shared ProducerOffsetReporter holder.
     *
     * <p>For UNDO, this holder is passed to the downstream SinkWriter via {@link
     * #getProducerOffsetReporter()} and delegates offset reports to the operator once registered.
     */
    private final ProducerOffsetReporterHolder offsetReporterHolder;

    // ==================== Constructors ====================

    /**
     * Creates a factory with an explicit failover correction action.
     *
     * @param tablePath the target table
     * @param flussConfig client configuration
     * @param tableRowType target row type
     * @param targetColumnIndexes partial-update targets, or null for full update
     * @param numBuckets table bucket count
     * @param isPartitioned whether the table is partitioned
     * @param producerId producer ID, or null to use the Flink job ID for UNDO only
     * @param recoveryAction failover correction action
     */
    public UndoRecoveryOperatorFactory(
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            @Nullable int[] targetColumnIndexes,
            int numBuckets,
            boolean isPartitioned,
            @Nullable String producerId,
            RecoveryAction recoveryAction) {
        this(
                tablePath,
                flussConfig,
                tableRowType,
                targetColumnIndexes,
                numBuckets,
                isPartitioned,
                producerId,
                recoveryAction,
                RecoveryOffsetManager.DEFAULT_PRODUCER_OFFSETS_POLL_INTERVAL_MS,
                RecoveryOffsetManager.DEFAULT_MAX_POLL_TIMEOUT_MS);
    }

    UndoRecoveryOperatorFactory(
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            @Nullable int[] targetColumnIndexes,
            int numBuckets,
            boolean isPartitioned,
            @Nullable String producerId,
            RecoveryAction recoveryAction,
            long producerOffsetsPollIntervalMs,
            long maxPollTimeoutMs) {
        this.tablePath = tablePath;
        this.flussConfig = flussConfig;
        this.tableRowType = tableRowType;
        this.targetColumnIndexes = targetColumnIndexes;
        this.numBuckets = numBuckets;
        this.isPartitioned = isPartitioned;
        this.producerId = producerId;
        this.recoveryAction = recoveryAction;
        this.producerOffsetsPollIntervalMs = producerOffsetsPollIntervalMs;
        this.maxPollTimeoutMs = maxPollTimeoutMs;

        // Create the shared holder that will be passed to both the writer and the operator
        this.offsetReporterHolder = new ProducerOffsetReporterHolder();

        // Set chaining strategy to ALWAYS to enable operator chaining
        // This allows the UndoRecoveryOperator to be chained with downstream operators
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    // ==================== StreamOperatorFactory Methods ====================

    /**
     * Creates a new {@link UndoRecoveryOperator} instance.
     *
     * <p>This method is called by Flink's runtime to create the operator instance. The created
     * operator is registered with the {@link ProducerOffsetReporterHolder} for UNDO so that offset
     * reports from the downstream SinkWriter are forwarded to the operator.
     *
     * @param parameters the stream operator parameters from Flink runtime
     * @param <T> the type of the stream operator
     * @return the created UndoRecoveryOperator instance
     */
    @Override
    public <T extends StreamOperator<IN>> T createStreamOperator(
            StreamOperatorParameters<IN> parameters) {
        UndoRecoveryOperator<IN> operator =
                new UndoRecoveryOperator<>(
                        parameters,
                        tablePath,
                        flussConfig,
                        tableRowType,
                        targetColumnIndexes,
                        numBuckets,
                        isPartitioned,
                        producerId,
                        recoveryAction,
                        producerOffsetsPollIntervalMs,
                        maxPollTimeoutMs,
                        offsetReporterHolder.getHolderId());

        if (recoveryAction == RecoveryAction.UNDO) {
            registerDelegate(offsetReporterHolder.getHolderId(), operator);
        }

        @SuppressWarnings("unchecked")
        final T castedOperator = (T) operator;

        return castedOperator;
    }

    /**
     * Returns the class of the stream operator created by this factory.
     *
     * @param classLoader the class loader to use
     * @return the UndoRecoveryOperator class
     */
    @SuppressWarnings("rawtypes")
    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return UndoRecoveryOperator.class;
    }

    // ==================== Getters ====================

    /**
     * Returns the ProducerOffsetReporter for an UNDO sink path.
     *
     * @return the ProducerOffsetReporter holder
     */
    public ProducerOffsetReporter getProducerOffsetReporter() {
        return offsetReporterHolder;
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public Configuration getFlussConfig() {
        return flussConfig;
    }

    public RowType getTableRowType() {
        return tableRowType;
    }

    @Nullable
    public int[] getTargetColumnIndexes() {
        return targetColumnIndexes;
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    public boolean isPartitioned() {
        return isPartitioned;
    }

    @Nullable
    public String getProducerId() {
        return producerId;
    }

    /**
     * Returns the failover correction action.
     *
     * @return NO_OP or UNDO
     */
    public RecoveryAction getRecoveryAction() {
        return recoveryAction;
    }

    public long getProducerOffsetsPollIntervalMs() {
        return producerOffsetsPollIntervalMs;
    }

    public long getMaxPollTimeoutMs() {
        return maxPollTimeoutMs;
    }

    /**
     * Registers a delegate in the static DELEGATE_REGISTRY by holder ID.
     *
     * <p>This is called by {@link UndoRecoveryOperator} during initialization to register itself so
     * that offset reports from the downstream SinkWriter are forwarded to the operator.
     *
     * @param holderId the holder ID to register under
     * @param delegate the delegate to register
     */
    public static void registerDelegate(String holderId, ProducerOffsetReporter delegate) {
        ProducerOffsetReporterHolder.registerDelegate(holderId, delegate);
    }

    /**
     * Removes a delegate from the static DELEGATE_REGISTRY by holder ID.
     *
     * <p>This should be called by {@link UndoRecoveryOperator#close()} to prevent memory leaks.
     * Without this cleanup, the static registry accumulates entries indefinitely as jobs are
     * submitted to long-running Flink clusters.
     *
     * @param holderId the holder ID to remove
     */
    public static void removeDelegate(String holderId) {
        ProducerOffsetReporterHolder.removeDelegate(holderId);
    }

    // ==================== Inner Classes ====================

    /**
     * A holder that acts as a bridge between the factory and the operator for offset reporting.
     *
     * <p>This holder is created during job construction and passed to the downstream SinkWriter.
     * When the operator is created at runtime, it registers itself with the holder via {@link
     * #setDelegate(ProducerOffsetReporter)}. Offset reports from the SinkWriter are then forwarded
     * to the actual operator.
     *
     * <p><b>Serialization Note:</b> This holder uses a static registry to maintain the connection
     * between the operator and writer across serialization boundaries. The holder is identified by
     * a unique ID that is preserved during serialization. When the operator is created, it
     * registers itself in the static registry. When the writer calls reportOffset(), the holder
     * looks up the delegate from the registry.
     */
    private static class ProducerOffsetReporterHolder
            implements ProducerOffsetReporter, Serializable {

        private static final long serialVersionUID = 1L;

        private static final Logger LOG =
                LoggerFactory.getLogger(ProducerOffsetReporterHolder.class);

        /**
         * Static registry mapping holder IDs to their delegates.
         *
         * <p>This registry is used to maintain the connection between the holder and its delegate
         * across serialization boundaries. The holder stores its ID, and when reportOffset() is
         * called, it looks up the delegate from this registry.
         */
        private static final Map<String, ProducerOffsetReporter> DELEGATE_REGISTRY =
                new ConcurrentHashMap<>();

        /** Unique ID for this holder, used to look up the delegate in the registry. */
        private final String holderId;

        /**
         * Cached delegate reference for hot-path optimization.
         *
         * <p>This volatile field caches the delegate after the first successful lookup from the
         * registry, avoiding repeated ConcurrentHashMap lookups on every {@link #reportOffset}
         * call. The field is volatile to ensure visibility across threads (async write callbacks).
         *
         * <p>Before serialization (same JVM): set directly by {@link #registerDelegate}. After
         * deserialization (different JVM): populated on first {@link #reportOffset} call from the
         * registry lookup.
         */
        @Nullable private transient volatile ProducerOffsetReporter cachedDelegate;

        ProducerOffsetReporterHolder() {
            this.holderId = UUID.randomUUID().toString();
            LOG.debug("Created ProducerOffsetReporterHolder with ID: {}", holderId);
        }

        String getHolderId() {
            return holderId;
        }

        @Override
        public void reportOffset(TableBucket bucket, long offset) {
            ProducerOffsetReporter delegate = cachedDelegate;
            if (delegate == null) {
                // After deserialization, cache from registry on first call
                delegate = DELEGATE_REGISTRY.get(holderId);
                if (delegate != null) {
                    cachedDelegate = delegate;
                }
            }
            if (delegate != null) {
                delegate.reportOffset(bucket, offset);
            } else {
                LOG.warn(
                        "No delegate found for holder ID: {}, offset report for bucket {} ignored",
                        holderId,
                        bucket);
            }
        }

        /**
         * Registers a delegate in the static registry by holder ID.
         *
         * @param holderId the holder ID to register under
         * @param delegate the delegate to register
         */
        static void registerDelegate(String holderId, ProducerOffsetReporter delegate) {
            DELEGATE_REGISTRY.put(holderId, delegate);
            LOG.debug("Registered delegate for holder ID: {}", holderId);
        }

        /**
         * Removes a delegate from the static registry by holder ID.
         *
         * @param holderId the holder ID to remove
         */
        static void removeDelegate(String holderId) {
            DELEGATE_REGISTRY.remove(holderId);
            LOG.debug("Removed delegate for holder ID: {}", holderId);
        }
    }
}
