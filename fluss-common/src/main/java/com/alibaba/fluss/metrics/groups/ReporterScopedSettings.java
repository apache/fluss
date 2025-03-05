/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.metrics.groups;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;

/** Encapsulates all settings that are defined per reporter. */
public class ReporterScopedSettings {

    private final int reporterIndex;

    public ReporterScopedSettings(int reporterIndex) {
        checkArgument(reporterIndex >= 0);
        this.reporterIndex = reporterIndex;
    }

    public int getReporterIndex() {
        return reporterIndex;
    }
}
