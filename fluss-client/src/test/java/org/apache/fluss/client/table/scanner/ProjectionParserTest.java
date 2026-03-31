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

package org.apache.fluss.client.table.scanner;

import org.apache.fluss.client.table.scanner.ProjectionParser.ParsedProjection;
import org.apache.fluss.client.table.scanner.ProjectionParser.ProjectedField;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ProjectionParser}. */
class ProjectionParserTest {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new org.apache.fluss.types.DataType[] {
                        DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.VARIANT()
                    },
                    new String[] {"id", "name", "data"});

    @Test
    void testRegularProjectionKeepsLegacyShape() {
        ParsedProjection parsed = ProjectionParser.parse(Arrays.asList("id", "name"), ROW_TYPE);
        assertThat(parsed.getProjectedColumns()).containsExactly(0, 1);
        assertThat(parsed.getVariantFieldProjection()).isNull();
        assertThat(parsed.getProjectedFields()).isNull();
    }

    @Test
    void testUntypedSubFieldProjection() {
        ParsedProjection parsed =
                ProjectionParser.parse(Arrays.asList("id", "data:name"), ROW_TYPE);
        assertThat(parsed.getProjectedColumns()).containsExactly(0, 2);
        assertThat(parsed.getVariantFieldProjection())
                .containsExactly(new java.util.AbstractMap.SimpleEntry<>(2, Arrays.asList("name")));
        List<ProjectedField> fields = parsed.getProjectedFields();
        assertThat(fields).hasSize(2);
        assertThat(fields.get(0).getDisplayName()).isEqualTo("id");
        assertThat(fields.get(0).isSubField()).isFalse();
        assertThat(fields.get(0).isCastField()).isFalse();
        assertThat(fields.get(1).getDisplayName()).isEqualTo("data_name");
        assertThat(fields.get(1).isSubField()).isTrue();
        assertThat(fields.get(1).isCastField()).isFalse();
        assertThat(fields.get(1).getSubFieldName()).isEqualTo("name");
    }

    @Test
    void testTypedSubFieldProjection() {
        ParsedProjection parsed =
                ProjectionParser.parse(
                        Arrays.asList("id", "data:name::STRING", "data:age::BIGINT"), ROW_TYPE);
        // Physical columns deduplicated: id, data
        assertThat(parsed.getProjectedColumns()).containsExactly(0, 2);
        // RPC hint aggregates both sub-fields under the original table column index.
        assertThat(parsed.getVariantFieldProjection()).containsKey(2);
        assertThat(parsed.getVariantFieldProjection().get(2))
                .containsExactlyInAnyOrder("name", "age");

        List<ProjectedField> fields = parsed.getProjectedFields();
        assertThat(fields).hasSize(3);

        assertThat(fields.get(0).getDisplayName()).isEqualTo("id");
        assertThat(fields.get(0).getCastType()).isNull();

        assertThat(fields.get(1).getDisplayName()).isEqualTo("data_name");
        assertThat(fields.get(1).getSubFieldName()).isEqualTo("name");
        assertThat(fields.get(1).getCastType().getTypeRoot()).isEqualTo(DataTypeRoot.STRING);
        assertThat(fields.get(1).getSourceProjectedPosition()).isEqualTo(1);

        assertThat(fields.get(2).getDisplayName()).isEqualTo("data_age");
        assertThat(fields.get(2).getSubFieldName()).isEqualTo("age");
        assertThat(fields.get(2).getCastType().getTypeRoot()).isEqualTo(DataTypeRoot.BIGINT);
        assertThat(fields.get(2).getSourceProjectedPosition()).isEqualTo(1);
    }

    @Test
    void testSubFieldProjectionUsesPhysicalInOrderPositionForFlatReader() {
        ParsedProjection parsed =
                ProjectionParser.parse(Arrays.asList("data:age::BIGINT", "id"), ROW_TYPE);

        assertThat(parsed.getProjectedColumns()).containsExactly(2, 0);
        assertThat(parsed.getVariantFieldProjection())
                .containsExactly(new java.util.AbstractMap.SimpleEntry<>(2, Arrays.asList("age")));

        List<ProjectedField> fields = parsed.getProjectedFields();
        assertThat(fields).hasSize(2);
        assertThat(fields.get(0).getDisplayName()).isEqualTo("data_age");
        // The server returns projected Arrow columns in ascending table-index order: id, data.
        assertThat(fields.get(0).getSourceProjectedPosition()).isEqualTo(1);
        assertThat(fields.get(1).getDisplayName()).isEqualTo("id");
        assertThat(fields.get(1).getSourceProjectedPosition()).isEqualTo(0);
    }

    @Test
    void testWholeColumnSupersedesSubFieldHint() {
        ParsedProjection parsed =
                ProjectionParser.parse(
                        Arrays.asList("data:name", "data", "data:age::BIGINT"), ROW_TYPE);
        assertThat(parsed.getProjectedColumns()).containsExactly(2);
        // Whole-column request clears the hint, so RPC sees the full Variant.
        assertThat(parsed.getVariantFieldProjection()).isNull();

        List<ProjectedField> fields = parsed.getProjectedFields();
        assertThat(fields).hasSize(3);
        assertThat(fields.get(0).isSubField()).isTrue();
        assertThat(fields.get(1).isSubField()).isFalse();
        assertThat(fields.get(2).isCastField()).isTrue();
    }

    @Test
    void testCastOnRegularColumnRejected() {
        assertThatThrownBy(() -> ProjectionParser.parse(Arrays.asList("id::BIGINT"), ROW_TYPE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cast suffix '::TYPE' is only supported");
    }

    @Test
    void testCastOnNonVariantColumnRejected() {
        assertThatThrownBy(
                        () -> ProjectionParser.parse(Arrays.asList("name:foo::STRING"), ROW_TYPE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a VARIANT type");
    }

    @Test
    void testNestedSubFieldProjectionRejectedInV1() {
        assertThatThrownBy(
                        () ->
                                ProjectionParser.parse(
                                        Arrays.asList("data:address.city::STRING"), ROW_TYPE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only top-level Variant fields are supported");

        assertThatThrownBy(
                        () ->
                                ProjectionParser.parse(
                                        Arrays.asList("data:scores[0]::BIGINT"), ROW_TYPE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only top-level Variant fields are supported");
    }

    @Test
    void testEmptyCastTypeRejected() {
        assertThatThrownBy(() -> ProjectionParser.parse(Arrays.asList("data:age::"), ROW_TYPE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cast type after '::' cannot be empty");
    }

    @Test
    void testNonScalarCastTypeRejected() {
        assertThatThrownBy(
                        () ->
                                ProjectionParser.parse(
                                        Arrays.asList("data:nested::ROW<a INT>"), ROW_TYPE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a supported scalar type");

        assertThatThrownBy(() -> ProjectionParser.parse(Arrays.asList("data:v::VARIANT"), ROW_TYPE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a supported scalar type");
    }

    @Test
    void testInvalidCastTypeReportsError() {
        assertThatThrownBy(
                        () ->
                                ProjectionParser.parse(
                                        Arrays.asList("data:age::NOT_A_TYPE"), ROW_TYPE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("failed to parse cast type");
    }
}
