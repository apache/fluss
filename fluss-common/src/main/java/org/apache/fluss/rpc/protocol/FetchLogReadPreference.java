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

import org.apache.fluss.annotation.Internal;

/** Read preference for fetch log requests. */
@Internal
public enum FetchLogReadPreference {
    LOCAL_FIRST(0, "local-first"),
    REMOTE_FIRST(1, "remote-first");

    private final int value;
    private final String name;

    FetchLogReadPreference(int value, String name) {
        this.value = value;
        this.name = name;
    }

    public int value() {
        return value;
    }

    public static FetchLogReadPreference from(int value) {
        for (FetchLogReadPreference preference : values()) {
            if (preference.value == value) {
                return preference;
            }
        }
        return LOCAL_FIRST;
    }

    @Override
    public String toString() {
        return name;
    }
}
