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

package com.alibaba.fluss.testutils.junit;

import com.alibaba.fluss.testutils.junit.parameterized.ParameterizedTestExtension;
import com.alibaba.fluss.testutils.junit.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for parameterized test on JUnit5 {@link ParameterizedTestExtension}. */
@ExtendWith(ParameterizedTestExtension.class)
public class ParameterizedTestExtensionTest {
    private static final List<Integer> PARAMETERS = Arrays.asList(1, 2);

    @Parameters
    private static List<Integer> parameters() {
        return PARAMETERS;
    }

    @TestTemplate
    void testWithParameters(int parameter) {
        assertThat(parameter).isIn(PARAMETERS);
    }
}
