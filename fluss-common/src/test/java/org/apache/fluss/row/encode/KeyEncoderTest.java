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

package org.apache.fluss.row.encode;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KeyEncoder}. */
class KeyEncoderTest {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.BIGINT()},
                    new String[] {"a", "b", "c"});

    static Stream<Arguments> tables() {
        return Stream.of(
                Arguments.of(1, null, true),
                Arguments.of(1, DataLakeFormat.LANCE, true),
                Arguments.of(1, DataLakeFormat.HUDI, true),
                Arguments.of(1, DataLakeFormat.PAIMON, false),
                Arguments.of(2, null, true),
                Arguments.of(2, DataLakeFormat.PAIMON, true),
                Arguments.of(2, DataLakeFormat.ICEBERG, true));
    }

    @ParameterizedTest(name = "kv format version {0}, lake format {1}")
    @MethodSource("tables")
    void testSupportsMultiKeyPrefixLookup(
            int kvFormatVersion, @Nullable DataLakeFormat lakeFormat, boolean supported) {
        TableConfig tableConfig = tableConfig(kvFormatVersion, lakeFormat);
        assertThat(KeyEncoder.supportsMultiKeyPrefixLookup(tableConfig)).isEqualTo(supported);

        // a string longer than 7 bytes isn't stored inline by the Paimon encoding
        InternalRow row = row(1L, "abcdefgh", 3L);
        byte[] prefix = primaryKey(tableConfig, "a", "b").encodeKey(row);
        byte[] key = primaryKey(tableConfig, "a", "b", "c").encodeKey(row);
        assertThat(Arrays.equals(Arrays.copyOf(key, prefix.length), prefix)).isEqualTo(supported);
    }

    @Test
    void testLegacyIcebergTableDoesNotSupportMultiKeyPrefixLookup() {
        // the legacy Iceberg encoding supports a single key column only
        assertThat(KeyEncoder.supportsMultiKeyPrefixLookup(tableConfig(1, DataLakeFormat.ICEBERG)))
                .isFalse();
    }

    private static KeyEncoder primaryKey(TableConfig tableConfig, String... keys) {
        return KeyEncoder.ofPrimaryKeyEncoder(ROW_TYPE, Arrays.asList(keys), tableConfig, false);
    }

    private static TableConfig tableConfig(
            int kvFormatVersion, @Nullable DataLakeFormat lakeFormat) {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.TABLE_KV_FORMAT_VERSION, kvFormatVersion);
        if (lakeFormat != null) {
            conf.set(ConfigOptions.TABLE_DATALAKE_FORMAT, lakeFormat);
        }
        return new TableConfig(conf);
    }
}
