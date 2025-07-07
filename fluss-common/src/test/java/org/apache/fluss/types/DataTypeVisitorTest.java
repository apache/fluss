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

/** Tests for the {@link org.apache.fluss.types.DataTypeVisitor} interface. */
public class DataTypeVisitorTest {

    @Test
    void testDataTypeDefaultVisitor() {
        DataType[] allTypes =
                new DataType[] {
                    DataTypes.BOOLEAN(),
                    DataTypes.TINYINT(),
                    DataTypes.SMALLINT(),
                    DataTypes.INT(),
                    DataTypes.BIGINT(),
                    DataTypes.DECIMAL(2, 1),
                    DataTypes.FLOAT(),
                    DataTypes.DOUBLE(),
                    DataTypes.DATE(),
                    DataTypes.TIME(),
                    DataTypes.TIMESTAMP(),
                    DataTypes.TIMESTAMP_LTZ(),
                    DataTypes.CHAR(5),
                    DataTypes.BINARY(10),
                    DataTypes.STRING(),
                    DataTypes.BYTES(),
                    DataTypes.ARRAY(DataTypes.INT()),
                    DataTypes.MAP(DataTypes.INT(), DataTypes.INT()),
                    DataTypes.ROW(DataTypes.FIELD("a", DataTypes.INT()))
                };

        String[] typeStrings = {
            "BOOLEAN",
            "TINYINT",
            "SMALLINT",
            "INT",
            "BIGINT",
            "DECIMAL(2, 1)",
            "FLOAT",
            "DOUBLE",
            "DATE",
            "TIME(0)",
            "TIMESTAMP(6)",
            "TIMESTAMP_LTZ(6)",
            "CHAR(5)",
            "BINARY(10)",
            "STRING",
            "BYTES",
            "ARRAY<INT>",
            "MAP<INT, INT>",
            "ROW<`a` INT>"
        };

        for (int i = 0; i < allTypes.length; i++) {
            assertThat(allTypes[i].accept(new ToStringVisitor())).isEqualTo(typeStrings[i]);
        }
    }

    // ----------------------------------------------------------------------------------------

    /** A toString() visitor for DataType. */
    static class ToStringVisitor extends DataTypeDefaultVisitor<String> {
        @Override
        protected String defaultMethod(DataType dataType) {
            return dataType.toString();
        }
    }
}
