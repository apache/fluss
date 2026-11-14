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

package org.apache.fluss.flink.action.orphan.job;

import org.apache.fluss.exception.TableNotExistException;

import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ScanAndCleanFunctionTest {

    @Test
    void forwardsScopeSummaryWithoutTouchingTheFilesystem() throws Exception {
        ScopeCoverageStats coverage = new ScopeCoverageStats();
        ScopeTargetCoverage target = ScopeTargetCoverage.forTarget(2L, false);
        target.disappeared(new TableNotExistException("gone"));
        coverage.add(target.finish());
        ScopeSummaryTask marker = new ScopeSummaryTask(coverage);
        List<CleanStats> output = new ArrayList<>();

        new ScanAndCleanFunction(1L, Collections.emptyMap())
                .processElement(marker, null, collectingInto(output));

        assertThat(output).hasSize(1);
        assertThat(output.get(0).counters())
                .usingRecursiveComparison()
                .isEqualTo(CleanupCounters.empty());
        assertThat(output.get(0).scopeCoverage().disappearedTableTargets()).isEqualTo(1L);
        assertThat(output.get(0).scopeCoverage().coverageComplete()).isTrue();
        assertThat(output.get(0).isScopeSummary()).isTrue();
    }

    private static Collector<CleanStats> collectingInto(List<CleanStats> output) {
        return new Collector<CleanStats>() {
            @Override
            public void collect(CleanStats record) {
                output.add(record);
            }

            @Override
            public void close() {}
        };
    }
}
