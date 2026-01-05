/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.testutils;

import org.apache.fluss.row.InternalMap;
import org.apache.fluss.types.MapType;

import org.assertj.core.api.AbstractAssert;

import static org.assertj.core.api.Assertions.assertThat;

/** AssertJ assert for {@link InternalMap}. */
public class InternalMapAssert extends AbstractAssert<InternalMapAssert, InternalMap> {

    private MapType mapType;

    InternalMapAssert(InternalMap actual) {
        super(actual, InternalMapAssert.class);
    }

    public static InternalMapAssert assertThatMap(InternalMap actual) {
        return new InternalMapAssert(actual);
    }

    public InternalMapAssert withMapType(MapType mapType) {
        this.mapType = mapType;
        return this;
    }

    public InternalMapAssert isEqualTo(InternalMap expected) {
        assert mapType != null;
        assertThat(actual.size()).as("Map size").isEqualTo(expected.size());

        for (int i = 0; i < expected.size(); i++) {
            Object expectedKey =
                    InternalArrayAssert.getValueAt(expected.keyArray(), i, mapType.getKeyType());
            Object expectedValue =
                    InternalArrayAssert.getValueAt(
                            expected.valueArray(), i, mapType.getValueType());

            boolean foundMatch = false;
            for (int j = 0; j < actual.size(); j++) {
                Object actualKey =
                        InternalArrayAssert.getValueAt(actual.keyArray(), j, mapType.getKeyType());
                if (InternalArrayAssert.valuesEqual(expectedKey, actualKey, mapType.getKeyType())) {
                    Object actualValue =
                            InternalArrayAssert.getValueAt(
                                    actual.valueArray(), j, mapType.getValueType());
                    if (InternalArrayAssert.valuesEqual(
                            expectedValue, actualValue, mapType.getValueType())) {
                        foundMatch = true;
                        break;
                    } else {
                        throw new AssertionError(
                                String.format(
                                        "Map has key %s but values don't match: expected=%s, actual=%s",
                                        expectedKey, expectedValue, actualValue));
                    }
                }
            }
            if (!foundMatch) {
                throw new AssertionError(
                        String.format(
                                "Expected map to contain key-value pair: %s=%s",
                                expectedKey, expectedValue));
            }
        }
        return this;
    }
}
