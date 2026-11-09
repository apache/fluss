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

package org.apache.fluss.flink.tiering.source.split;

import org.apache.fluss.client.tiering.TieringSplit;

import org.apache.flink.api.connector.source.SourceSplit;

import java.util.Objects;

/**
 * A Flink-specific wrapper for {@link TieringSplit} that implements Flink's {@link SourceSplit}
 * interface. This adapter allows the engine-agnostic {@link TieringSplit} to be used within Flink's
 * source framework.
 */
public class FlinkTieringSplit implements SourceSplit {

    private static final long serialVersionUID = 1L;

    private final TieringSplit tieringSplit;

    /** Wraps an engine-agnostic split for the Flink source framework. */
    public FlinkTieringSplit(TieringSplit tieringSplit) {
        this.tieringSplit = tieringSplit;
    }

    @Override
    public String splitId() {
        return tieringSplit.splitId();
    }

    /** Returns the underlying engine-agnostic {@link TieringSplit}. */
    public TieringSplit unwrap() {
        return tieringSplit;
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof FlinkTieringSplit)) {
            return false;
        }
        FlinkTieringSplit that = (FlinkTieringSplit) object;
        return Objects.equals(tieringSplit, that.tieringSplit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tieringSplit);
    }

    @Override
    public String toString() {
        return "FlinkTieringSplit{" + tieringSplit + '}';
    }
}
