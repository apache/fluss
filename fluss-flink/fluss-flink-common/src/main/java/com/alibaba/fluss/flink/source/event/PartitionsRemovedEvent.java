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

package com.alibaba.fluss.flink.source.event;

import org.apache.flink.api.connector.source.SourceEvent;

import java.util.Map;

/**
 * A source event to represent partitions is removed to send from enumerator to reader.
 *
 * <p>It contains the partition bucket of the removed partitions that has been assigned to the
 * reader.
 */
public class PartitionsRemovedEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final Map<Long, String> removedPartitions;

    public PartitionsRemovedEvent(Map<Long, String> removedPartitions) {
        this.removedPartitions = removedPartitions;
    }

    public Map<Long, String> getRemovedPartitions() {
        return removedPartitions;
    }

    @Override
    public String toString() {
        return "PartitionsRemovedEvent{" + "removedPartitions=" + removedPartitions + '}';
    }
}
