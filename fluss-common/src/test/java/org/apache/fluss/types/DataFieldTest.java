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

package org.apache.fluss.types;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link org.apache.fluss.types.DataField} class. */
public class DataFieldTest {

    @Test
    void testCreateField() {
        DataField fieldA = new DataField("a", new CharType(5));
        assertThat(fieldA.getName()).isEqualTo("a");
        assertThat(fieldA.getType()).isInstanceOf(CharType.class);
        assertThat(fieldA.getDescription()).isEmpty();
        assertThat(fieldA.asSerializableString()).isEqualTo("`a` CHAR(5)");
        assertThat(fieldA.asSummaryString()).isEqualTo("`a` CHAR(5)");

        DataField fieldB = new DataField("b", new IntType(), "column b");
        assertThat(fieldB.getName()).isEqualTo("b");
        assertThat(fieldB.getType()).isInstanceOf(IntType.class);
        assertThat(fieldB.getDescription().orElse("")).isEqualTo("column b");
        assertThat(fieldB.asSerializableString()).isEqualTo("`b` INT 'column b'");
        assertThat(fieldB.asSummaryString()).isEqualTo("`b` INT '...'");
    }
}
