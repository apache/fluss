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

package org.apache.fluss.config;

import org.apache.fluss.metadata.DeleteBehavior;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TableConfig}. */
class TableConfigTest {

    @Test
    void testDeleteBehavior() {
        Configuration conf = new Configuration();
        TableConfig tableConfig = new TableConfig(conf);

        // Test default value (empty optional since not set)
        assertThat(tableConfig.getDeleteBehavior()).isEmpty();

        // Test configured value
        conf.set(ConfigOptions.TABLE_DELETE_BEHAVIOR, DeleteBehavior.ALLOW);
        TableConfig tableConfig2 = new TableConfig(conf);
        assertThat(tableConfig2.getDeleteBehavior()).hasValue(DeleteBehavior.ALLOW);

        // Test IGNORE
        conf.set(ConfigOptions.TABLE_DELETE_BEHAVIOR, DeleteBehavior.IGNORE);
        TableConfig tableConfig3 = new TableConfig(conf);
        assertThat(tableConfig3.getDeleteBehavior()).hasValue(DeleteBehavior.IGNORE);
    }
}
