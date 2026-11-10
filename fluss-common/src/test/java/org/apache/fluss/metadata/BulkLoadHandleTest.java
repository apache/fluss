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

package org.apache.fluss.metadata;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BulkLoadHandleTest {
    private static final String ID = "550e8400-e29b-41d4-a716-446655440000";

    @Test
    void testTableIdentity() {
        TablePath path = TablePath.of("db", "table");
        BulkLoadHandle handle = new BulkLoadHandle(path, 41L, ID);
        assertThat(handle.getTablePath()).isEqualTo(path);
        assertThat(handle.getTableId()).isEqualTo(41L);
        assertThat(handle.getBulkLoadId()).isEqualTo(ID);
        assertThat(handle).isEqualTo(new BulkLoadHandle(path, 41L, ID));
        assertThat(handle).hasSameHashCodeAs(new BulkLoadHandle(path, 41L, ID));
        assertThat(handle).isNotEqualTo(new BulkLoadHandle(path, 42L, ID));
        assertThat(handle).isNotEqualTo(new BulkLoadHandle(TablePath.of("db", "other"), 41L, ID));
    }

    @Test
    void testRejectInvalidIdentity() {
        assertThatThrownBy(() -> new BulkLoadHandle(null, 41L, ID))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new BulkLoadHandle(TablePath.of("", "table"), 41L, ID))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BulkLoadHandle(TablePath.of("db", "table"), -1L, ID))
                .isInstanceOf(IllegalArgumentException.class);
        for (String id : new String[] {"invalid", ID.toUpperCase(), "1-1-1-1-1"}) {
            assertThatThrownBy(() -> new BulkLoadHandle(TablePath.of("db", "table"), 41L, id))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
