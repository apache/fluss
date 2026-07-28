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

package org.apache.fluss.client.metrics;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.write.WriterClient;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics for {@link WriterClient}s owned by multi-table writers.
 *
 * <p>Unlike the connection-level shared writer client which keeps the {@code client.writer} metric
 * identity, each multi-table writer owns a dedicated {@link WriterClient}, so multiple instances
 * may coexist under the same {@link ClientMetricGroup}. This group therefore uses the dedicated
 * {@code multi_table_writer} scope plus a unique {@code writer_instance_id} variable so that
 * reporters derive distinct metric identities (e.g. JMX ObjectNames, Prometheus label sets) instead
 * of colliding.
 */
@Internal
public class MultiTableWriterMetricGroup extends WriterMetricGroup {

    private static final String NAME = "multi_table_writer";
    private static final AtomicLong NEXT_WRITER_INSTANCE_ID = new AtomicLong();

    public MultiTableWriterMetricGroup(ClientMetricGroup parent) {
        super(parent, NAME, String.valueOf(NEXT_WRITER_INSTANCE_ID.incrementAndGet()));
    }
}
