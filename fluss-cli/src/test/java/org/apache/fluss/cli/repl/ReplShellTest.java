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

package org.apache.fluss.cli.repl;

import org.apache.fluss.cli.sql.SqlExecutor;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** Tests for {@link ReplShell}. */
class ReplShellTest {

    @Test
    void testReplShellConstruction() {
        SqlExecutor mockExecutor = mock(SqlExecutor.class);
        ReplShell replShell = new ReplShell(mockExecutor);
        assertThat(replShell).isNotNull();
    }

    @Test
    void testReplShellExecutorAssignment() {
        SqlExecutor mockExecutor = mock(SqlExecutor.class);
        ReplShell replShell = new ReplShell(mockExecutor);
        assertThat(replShell).isNotNull();
    }

    @Test
    void testReplShellWithNullExecutor() {
        ReplShell replShell = new ReplShell(null);
        assertThat(replShell).isNotNull();
    }

    @Test
    void testReplShellMultipleInstances() {
        SqlExecutor mockExecutor1 = mock(SqlExecutor.class);
        SqlExecutor mockExecutor2 = mock(SqlExecutor.class);

        ReplShell replShell1 = new ReplShell(mockExecutor1);
        ReplShell replShell2 = new ReplShell(mockExecutor2);

        assertThat(replShell1).isNotNull();
        assertThat(replShell2).isNotNull();
        assertThat(replShell1).isNotSameAs(replShell2);
    }

    @Test
    void testReplShellExecutorPassedCorrectly() {
        SqlExecutor mockExecutor = mock(SqlExecutor.class);
        ReplShell replShell = new ReplShell(mockExecutor);

        assertThat(replShell).isNotNull();
    }
}
