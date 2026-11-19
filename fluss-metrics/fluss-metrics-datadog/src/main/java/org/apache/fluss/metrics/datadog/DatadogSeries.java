/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.metrics.datadog;

import javax.annotation.Nullable;

import java.util.List;

/** One data point of one metric, as sent to the Datadog series API. */
final class DatadogSeries {

    /** The type of a series, with the code the Datadog v2 series API uses for it. */
    enum Type {
        COUNT(1),
        GAUGE(3);

        private final int code;

        Type(int code) {
            this.code = code;
        }

        int code() {
            return code;
        }
    }

    private final String metric;
    private final Type type;
    private final double value;
    private final List<String> tags;
    @Nullable private final String host;

    DatadogSeries(
            String metric, Type type, double value, List<String> tags, @Nullable String host) {
        this.metric = metric;
        this.type = type;
        this.value = value;
        this.tags = tags;
        this.host = host;
    }

    String metric() {
        return metric;
    }

    Type type() {
        return type;
    }

    double value() {
        return value;
    }

    List<String> tags() {
        return tags;
    }

    @Nullable
    String host() {
        return host;
    }
}
