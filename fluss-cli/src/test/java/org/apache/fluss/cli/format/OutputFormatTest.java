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

package org.apache.fluss.cli.format;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutputFormatTest {

    @Test
    void testFromStringTable() {
        OutputFormat format = OutputFormat.fromString("table");
        assertThat(format).isEqualTo(OutputFormat.TABLE);
    }

    @Test
    void testFromStringCsv() {
        OutputFormat format = OutputFormat.fromString("csv");
        assertThat(format).isEqualTo(OutputFormat.CSV);
    }

    @Test
    void testFromStringJson() {
        OutputFormat format = OutputFormat.fromString("json");
        assertThat(format).isEqualTo(OutputFormat.JSON);
    }

    @Test
    void testFromStringTsv() {
        OutputFormat format = OutputFormat.fromString("tsv");
        assertThat(format).isEqualTo(OutputFormat.TSV);
    }

    @Test
    void testFromStringCaseInsensitive() {
        assertThat(OutputFormat.fromString("TABLE")).isEqualTo(OutputFormat.TABLE);
        assertThat(OutputFormat.fromString("CSV")).isEqualTo(OutputFormat.CSV);
        assertThat(OutputFormat.fromString("JSON")).isEqualTo(OutputFormat.JSON);
        assertThat(OutputFormat.fromString("TSV")).isEqualTo(OutputFormat.TSV);
        assertThat(OutputFormat.fromString("TaBLe")).isEqualTo(OutputFormat.TABLE);
    }

    @Test
    void testFromStringInvalid() {
        assertThatThrownBy(() -> OutputFormat.fromString("xml"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid output format");
    }

    @Test
    void testFromStringNull() {
        OutputFormat format = OutputFormat.fromString(null);
        assertThat(format).isEqualTo(OutputFormat.TABLE);
    }

    @Test
    void testFromStringEmpty() {
        assertThatThrownBy(() -> OutputFormat.fromString(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid output format");
    }
}
