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

import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.InstantiationUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ColumnPath}. */
class ColumnPathTest {

    @Test
    void testParseDottedPath() {
        ColumnPath path = ColumnPath.parse("profile.address.city");

        assertThat(path.getSegments()).hasSize(3);
        assertThat(path.isNested()).isTrue();
        assertThat(path.toString()).isEqualTo("profile.address.city");
    }

    @Test
    void testArrayElementSegment() {
        ColumnPath path =
                ColumnPath.of(ColumnPath.Segment.field("items"), ColumnPath.Segment.arrayElement());

        assertThat(path.toString()).isEqualTo("items.$element");
        assertThat(path.getSegments().get(1).getName()).isNotPresent();
    }

    @Test
    void testMapValueSegment() {
        ColumnPath path =
                ColumnPath.of(ColumnPath.Segment.field("attrs"), ColumnPath.Segment.mapValue());

        assertThat(path.toString()).isEqualTo("attrs.$value");
        assertThat(path.getSegments().get(1).getName()).isNotPresent();
    }

    @Test
    void testFieldSegmentName() {
        ColumnPath.Segment segment = ColumnPath.Segment.field("profile");

        assertThat(segment.getName()).hasValue("profile");
    }

    @Test
    void testInvalidFieldName() {
        assertThatThrownBy(() -> ColumnPath.Segment.field(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be empty");
    }

    @Test
    void testRejectEmptyPath() {
        assertThatThrownBy(() -> ColumnPath.of(new ColumnPath.Segment[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must contain at least one segment");
    }

    @Test
    void testPathIsImmutable() {
        List<ColumnPath.Segment> segments =
                new ArrayList<>(Arrays.asList(ColumnPath.Segment.field("profile")));
        ColumnPath path = ColumnPath.of(segments);
        segments.add(ColumnPath.Segment.field("address"));

        assertThat(path.isNested()).isFalse();
        assertThat(path).isEqualTo(ColumnPath.of("profile"));
        assertThatThrownBy(() -> path.getSegments().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testPathEqualityAndSerialization() throws Exception {
        ColumnPath path = ColumnPath.parse("profile.address");
        assertThat(path).isEqualTo(path).isEqualTo(ColumnPath.of("profile", "address"));
        assertThat(path).hasSameHashCodeAs(ColumnPath.of("profile", "address"));
        assertThat(path).isNotEqualTo(null).isNotEqualTo("profile.address");
        assertThat(path).isNotEqualTo(ColumnPath.of("address", "profile"));
        ColumnPath mixedPath =
                ColumnPath.of(
                        ColumnPath.Segment.field("items"),
                        ColumnPath.Segment.arrayElement(),
                        ColumnPath.Segment.mapValue());
        assertThat(InstantiationUtils.clone(mixedPath)).isEqualTo(mixedPath);
    }

    @Test
    void testSegmentEquality() {
        ColumnPath.Segment field = ColumnPath.Segment.field("items");
        assertThat(field).isEqualTo(field).isEqualTo(ColumnPath.Segment.field("items"));
        assertThat(field).hasSameHashCodeAs(ColumnPath.Segment.field("items"));
        assertThat(field).isNotEqualTo(ColumnPath.Segment.field("other"));
        assertThat(field).isNotEqualTo(ColumnPath.Segment.arrayElement());
        assertThat(field).isNotEqualTo(null).isNotEqualTo("items");
        assertThat(field.getType()).isEqualTo(ColumnPath.SegmentType.FIELD);
        assertThat(field.toString()).isEqualTo("items");
        assertThat(ColumnPath.Segment.arrayElement()).isEqualTo(ColumnPath.Segment.arrayElement());
        assertThat(ColumnPath.Segment.mapValue().toString()).isEqualTo("$value");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", ".", ".profile", "profile..address", "profile.", "profile.."})
    void testRejectInvalidDottedPath(String path) {
        assertThatThrownBy(() -> ColumnPath.parse(path))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testRejectNullPathInputs() {
        assertThatThrownBy(() -> ColumnPath.parse(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> ColumnPath.of((String[]) null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> ColumnPath.of((ColumnPath.Segment[]) null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> ColumnPath.of((List<ColumnPath.Segment>) null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> ColumnPath.Segment.field(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> ColumnPath.of(ColumnPath.Segment.field("profile"), null))
                .isInstanceOf(NullPointerException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " "})
    void testRejectEmptyNestedFieldName(String fieldName) {
        assertThatThrownBy(
                        () ->
                                TableChange.addNestedField(
                                        ColumnPath.of("profile"), fieldName, DataTypes.INT(), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testRejectNullNestedFieldInputs() {
        assertThatThrownBy(() -> TableChange.addNestedField(null, "age", DataTypes.INT(), null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(
                        () ->
                                TableChange.addNestedField(
                                        ColumnPath.of("profile"), null, DataTypes.INT(), null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(
                        () ->
                                TableChange.addNestedField(
                                        ColumnPath.of("profile"), "age", null, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testNestedFieldChange() {
        ColumnPath parent = ColumnPath.of("profile", "address");
        TableChange.AddNestedField change =
                TableChange.addNestedField(parent, "country", DataTypes.STRING(), "country name");
        assertThat(change).isInstanceOf(TableChange.SchemaChange.class);
        assertThat(change.getParentPath()).isEqualTo(parent);
        assertThat(change.getFieldName()).isEqualTo("country");
        assertThat(change.getDataType()).isEqualTo(DataTypes.STRING());
        assertThat(change.getComment()).isEqualTo("country name");
        assertThat(change.getFullPath()).isEqualTo("profile.address.country");
        assertThat(change.toString()).contains("profile.address", "country", "country name");
        assertThat(TableChange.addNestedField(parent, "zip", DataTypes.INT(), null).getComment())
                .isNull();
    }
}
