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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

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
 * <p><b>ProducerOffsetReporter:</b> The factory owns a registry namespace that is preserved across
 * its independently serialized copies. At runtime, each operator and Sink Writer combines that
 * namespace with its subtask index. This enables every Sink Writer to report offsets only to the
 * corresponding UndoRecoveryOperator.
 *
 * @param <IN> The type of input elements
 * @see UndoRecoveryOperator
 * @see ProducerOffsetReporter
 */
@Internal
public class UndoRecoveryOperatorFactory<IN> extends AbstractStreamOperatorFactory<IN>
        implements OneInputStreamOperatorFactory<IN, IN> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(UndoRecoveryOperatorFactory.class);

    /** Runtime registry mapping a namespace and subtask index to the corresponding operator. */
    private static final Map<String, ProducerOffsetReporter> DELEGATE_REGISTRY =
            new ConcurrentHashMap<>();

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
     * for pre-checkpoint failure recovery. If null, the operator will use the Flink job ID.
     */
    @Nullable private final String producerId;

    /** The polling interval in milliseconds for producer offsets synchronization. */
    private final long producerOffsetsPollIntervalMs;

    /** The maximum total time in milliseconds to poll for producer offsets before giving up. */
    private final long maxPollTimeoutMs;

    // ==================== Reporter Registry Fields ====================

    /** Registry namespace shared by independently serialized copies of this factory. */
    private final String offsetReporterRegistryNamespace;

    // ==================== Constructors ====================

    /**
     * Creates a new UndoRecoveryOperatorFactory with default producer offset poll interval.
     *
     * @param tablePath the table path for the Fluss table
     * @param flussConfig the Fluss configuration
     * @param tableRowType the row type of the table
     * @param targetColumnIndexes target column indexes for partial update (null for full row)
     * @param numBuckets the number of buckets in the table
     * @param isPartitioned whether the table is partitioned
     * @param producerId the producer ID for producer offset management (null to use Flink job ID)
     */
    public UndoRecoveryOperatorFactory(
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            @Nullable int[] targetColumnIndexes,
            int numBuckets,
            boolean isPartitioned,
            @Nullable String producerId) {
        this(
                tablePath,
                flussConfig,
                tableRowType,
                targetColumnIndexes,
                numBuckets,
                isPartitioned,
                producerId,
                RecoveryOffsetManager.DEFAULT_PRODUCER_OFFSETS_POLL_INTERVAL_MS,
                RecoveryOffsetManager.DEFAULT_MAX_POLL_TIMEOUT_MS);
    }

    /**
     * Creates a new UndoRecoveryOperatorFactory.
     *
     * <p>The factory is configured with {@link ChainingStrategy#ALWAYS} to enable operator chaining
     * with downstream operators for better performance.
     *
     * @param tablePath the table path for the Fluss table
     * @param flussConfig the Fluss configuration
     * @param tableRowType the row type of the table
     * @param targetColumnIndexes target column indexes for partial update (null for full row)
     * @param numBuckets the number of buckets in the table
     * @param isPartitioned whether the table is partitioned
     * @param producerId the producer ID for producer offset management (null to use Flink job ID)
     * @param producerOffsetsPollIntervalMs the polling interval for producer offsets
     * @param maxPollTimeoutMs the maximum total time to poll for producer offsets
     */
    public UndoRecoveryOperatorFactory(
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            @Nullable int[] targetColumnIndexes,
            int numBuckets,
            boolean isPartitioned,
            @Nullable String producerId,
            long producerOffsetsPollIntervalMs,
            long maxPollTimeoutMs) {
        this.tablePath = tablePath;
        this.flussConfig = flussConfig;
        this.tableRowType = tableRowType;
        this.targetColumnIndexes = targetColumnIndexes;
        this.numBuckets = numBuckets;
        this.isPartitioned = isPartitioned;
        this.producerId = producerId;
        this.producerOffsetsPollIntervalMs = producerOffsetsPollIntervalMs;
        this.maxPollTimeoutMs = maxPollTimeoutMs;

        this.offsetReporterRegistryNamespace = UUID.randomUUID().toString();

        // Set chaining strategy to ALWAYS to enable operator chaining
        // This allows the UndoRecoveryOperator to be chained with downstream operators
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    // ==================== StreamOperatorFactory Methods ====================

    /**
     * Creates a new {@link UndoRecoveryOperator} instance.
     *
     * <p>This method is called by Flink's runtime to create the operator instance. The created
     * operator is registered under the namespace-and-subtask registry ID so that offset reports
     * from the corresponding downstream SinkWriter are forwarded to this operator.
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
                        producerOffsetsPollIntervalMs,
                        maxPollTimeoutMs,
                        offsetReporterRegistryNamespace);

        // Register the operator with the static registry so offset reports are forwarded
        registerDelegate(operator.getOffsetReporterRegistryId(), operator);

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

    public long getProducerOffsetsPollIntervalMs() {
        return producerOffsetsPollIntervalMs;
    }

    public long getMaxPollTimeoutMs() {
        return maxPollTimeoutMs;
    }

    /**
     * Creates a runtime reporter bound to one Sink Writer subtask.
     *
     * @param subtaskIndex the runtime Sink Writer subtask index
     * @return a reporter that forwards offsets to the matching UndoRecoveryOperator
     */
    public ProducerOffsetReporter createProducerOffsetReporter(int subtaskIndex) {
        return new BoundProducerOffsetReporter(
                createOffsetReporterRegistryId(offsetReporterRegistryNamespace, subtaskIndex));
    }

    static String createOffsetReporterRegistryId(String registryNamespace, int subtaskIndex) {
        checkNotNull(
                registryNamespace, "Producer offset reporter registry namespace must not be null");
        checkArgument(
                subtaskIndex >= 0,
                "Producer offset reporter subtask index must be non-negative, but was %s",
                subtaskIndex);
        return registryNamespace + ":" + subtaskIndex;
    }

    /**
     * Registers a delegate in the runtime registry.
     *
     * <p>Normal failover may replace a stale mapping left by the previous execution attempt, so
     * registration deliberately uses last-writer-wins semantics. Concurrent attempts writing the
     * same bucket are outside the supported correctness boundary.
     *
     * @param registryId the namespace-and-subtask registry ID
     * @param delegate the delegate to register
     */
    static void registerDelegate(String registryId, ProducerOffsetReporter delegate) {
        DELEGATE_REGISTRY.put(
                checkNotNull(registryId, "Registry ID must not be null"),
                checkNotNull(delegate, "Delegate must not be null"));
        LOG.debug("Registered producer offset reporter delegate for registry ID: {}", registryId);
    }

    /**
     * Removes a delegate from the runtime registry if it is still the registered owner.
     *
     * <p>The ownership check prevents a delayed close from deleting a replacement registered by a
     * later execution attempt.
     *
     * @param registryId the namespace-and-subtask registry ID
     * @param delegate the delegate that owns the registration
     */
    static void removeDelegate(String registryId, ProducerOffsetReporter delegate) {
        boolean removed = DELEGATE_REGISTRY.remove(registryId, delegate);
        LOG.debug(
                "Removed producer offset reporter delegate for registry ID {}: {}",
                registryId,
                removed);
    }

    /** Runtime reporter whose registry identity is fixed when its Sink Writer is created. */
    private static final class BoundProducerOffsetReporter implements ProducerOffsetReporter {

        private final String registryId;

        /** Cached delegate for the async write-result hot path. */
        @Nullable private volatile ProducerOffsetReporter cachedDelegate;

        private BoundProducerOffsetReporter(String registryId) {
            this.registryId = registryId;
        }

        @Override
        public void reportOffset(TableBucket bucket, long offset) {
            ProducerOffsetReporter delegate = cachedDelegate;
            if (delegate == null) {
                delegate = DELEGATE_REGISTRY.get(registryId);
                if (delegate != null) {
                    cachedDelegate = delegate;
                }
            }
            checkState(
                    delegate != null,
                    "No delegate found for reporter registry ID %s; offset report for bucket %s cannot be delivered",
                    registryId,
                    bucket);
            delegate.reportOffset(bucket, offset);
        }
    }
}
