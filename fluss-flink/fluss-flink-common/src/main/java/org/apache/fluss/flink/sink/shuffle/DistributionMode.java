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

package org.apache.fluss.flink.sink.shuffle;

import java.util.Locale;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Distribution mode for sink shuffling. */
public enum DistributionMode {
    NONE("NONE"),
    BUCKET_SHUFFLE("BUCKET_SHUFFLE"),
    DYNAMIC_SHUFFLE("DYNAMIC_SHUFFLE");

    private final String modeName;

    DistributionMode(String modeName) {
        this.modeName = modeName;
    }

    public String modeName() {
        return modeName;
    }

    public static DistributionMode fromName(String modeName) {
        checkArgument(null != modeName, "Invalid distribution mode: null");
        try {
            return DistributionMode.valueOf(modeName.toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid distribution mode: %s", modeName));
        }
    }
}
