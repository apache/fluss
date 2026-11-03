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

package org.apache.fluss.server.kv;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;

import javax.annotation.concurrent.ThreadSafe;

/** Dynamically updatable KV settings shared by a manager and its tablets. */
@Internal
@ThreadSafe
public final class KvConfig {

    private volatile boolean batchOldValueLookupEnabled;

    /** Creates KV settings from the supplied configuration. */
    public KvConfig(Configuration configuration) {
        update(configuration);
    }

    /** Updates the shared settings from the supplied configuration. */
    public void update(Configuration configuration) {
        batchOldValueLookupEnabled =
                configuration.get(ConfigOptions.KV_WRITE_BATCH_OLD_VALUE_LOOKUP_ENABLED);
    }

    /** Returns whether write batches may prefetch their old values. */
    public boolean isBatchOldValueLookupEnabled() {
        return batchOldValueLookupEnabled;
    }
}
