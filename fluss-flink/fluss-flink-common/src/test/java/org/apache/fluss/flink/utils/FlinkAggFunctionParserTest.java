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

package org.apache.fluss.flink.utils;

import org.apache.fluss.metadata.AggFunction;
import org.apache.fluss.metadata.AggFunctionType;
import org.apache.fluss.metadata.AggFunctions;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FlinkAggFunctionParser}. */
class FlinkAggFunctionParserTest {

    @Test
    void testParseNoAggFunction() {
        Configuration options = new Configuration();
        assertThat(FlinkAggFunctionParser.parseAggFunction("total", options)).isEmpty();
    }

    @Test
    void testParseFunctionWithoutParameters() {
        Configuration options = new Configuration();
        options.setString("fields.total.agg", "sum");

        Optional<AggFunction> result = FlinkAggFunctionParser.parseAggFunction("total", options);

        assertThat(result).isPresent();
        assertThat(result.get().getType()).isEqualTo(AggFunctionType.SUM);
        assertThat(result.get().hasParameters()).isFalse();
    }

    @Test
    void testParseFunctionWithParameters() {
        Configuration options = new Configuration();
        options.setString("fields.tags.agg", "listagg");
        options.setString("fields.tags.listagg.delimiter", ";");

        Optional<AggFunction> result = FlinkAggFunctionParser.parseAggFunction("tags", options);

        assertThat(result).isPresent();
        assertThat(result.get().getType()).isEqualTo(AggFunctionType.LISTAGG);
        assertThat(result.get().getParameter("delimiter")).contains(";");

        Map<String, String> newOptions = new HashMap<>();
        FlinkAggFunctionParser.formatAggFunctionToOptions(
                "tags", AggFunctions.LISTAGG(";"), newOptions);
        assertThat(newOptions).isEqualTo(options.toMap());
    }

    @Test
    void testParseFunctionWithUpperCaseName() {
        Configuration options = new Configuration();
        options.setString("fields.tags.agg", "LISTAGG");
        options.setString("fields.tags.listagg.delimiter", ";");

        Optional<AggFunction> result = FlinkAggFunctionParser.parseAggFunction("tags", options);

        assertThat(result).isPresent();
        assertThat(result.get().getType()).isEqualTo(AggFunctionType.LISTAGG);
        assertThat(result.get().getParameter("delimiter")).contains(";");
    }

    @Test
    void testParseColumnNameIsolation() {
        // Test that configurations for different columns don't interfere with each other
        Configuration options = new Configuration();
        options.setString("fields.col1.agg", "sum");
        options.setString("fields.col2.agg", "listagg");
        options.setString("fields.col2.listagg.delimiter", "|"); // This should not affect col1

        Optional<AggFunction> col1Func = FlinkAggFunctionParser.parseAggFunction("col1", options);
        Optional<AggFunction> col2Func = FlinkAggFunctionParser.parseAggFunction("col2", options);
        Optional<AggFunction> col3Func = FlinkAggFunctionParser.parseAggFunction("col3", options);

        // col1 should have SUM without parameters
        assertThat(col1Func).isPresent();
        assertThat(col1Func.get().getType()).isEqualTo(AggFunctionType.SUM);
        assertThat(col1Func.get().hasParameters()).isFalse();

        // col2 should have LISTAGG with delimiter parameter
        assertThat(col2Func).isPresent();
        assertThat(col2Func.get().getType()).isEqualTo(AggFunctionType.LISTAGG);
        assertThat(col2Func.get().getParameter("delimiter")).contains("|");

        // col3 should have no configuration
        assertThat(col3Func).isEmpty();
    }

    @Test
    void testParseInvalidFunctionName() {
        Configuration options = new Configuration();
        options.setString("fields.total.agg", "invalid_function");

        assertThatThrownBy(() -> FlinkAggFunctionParser.parseAggFunction("total", options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown aggregation function")
                .hasMessageContaining("invalid_function");
    }

    @Test
    void testParseEmptyFunctionName() {
        Configuration options = new Configuration();
        options.setString("fields.total.agg", "");

        assertThatThrownBy(() -> FlinkAggFunctionParser.parseAggFunction("total", options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Empty aggregation function name");
    }

    @Test
    void testRoundTripConversion() {
        // Test that parse and format are inverse operations
        Map<String, String> options = new HashMap<>();

        // Format functions to options
        FlinkAggFunctionParser.formatAggFunctionToOptions("col1", AggFunctions.SUM(), options);
        FlinkAggFunctionParser.formatAggFunctionToOptions(
                "col2", AggFunctions.LISTAGG(";"), options);

        // Parse them back
        Configuration config = Configuration.fromMap(options);
        Optional<AggFunction> col1Func = FlinkAggFunctionParser.parseAggFunction("col1", config);
        Optional<AggFunction> col2Func = FlinkAggFunctionParser.parseAggFunction("col2", config);

        // Verify they match the original functions
        assertThat(col1Func).isPresent();
        assertThat(col1Func.get()).isEqualTo(AggFunctions.SUM());

        assertThat(col2Func).isPresent();
        assertThat(col2Func.get()).isEqualTo(AggFunctions.LISTAGG(";"));
    }
}
