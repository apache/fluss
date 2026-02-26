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

package org.apache.fluss.lake.lance.utils;

import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LanceArrowUtils#toArrowSchema(RowType, Map)}. */
class LanceArrowUtilsTest {

    @Test
    void testArrayColumnWithoutProperty() {
        RowType rowType =
                DataTypes.ROW(DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT())));

        Schema schema = LanceArrowUtils.toArrowSchema(rowType, Collections.emptyMap());

        Field embeddingField = schema.findField("embedding");
        assertThat(embeddingField.getType()).isInstanceOf(ArrowType.List.class);
    }

    @Test
    void testArrayColumnWithFixedSizeListProperty() {
        RowType rowType =
                DataTypes.ROW(DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT())));

        Map<String, String> properties = new HashMap<>();
        properties.put("embedding.arrow.fixed-size-list.size", "4");

        Schema schema = LanceArrowUtils.toArrowSchema(rowType, properties);

        Field embeddingField = schema.findField("embedding");
        assertThat(embeddingField.getType()).isInstanceOf(ArrowType.FixedSizeList.class);
        assertThat(((ArrowType.FixedSizeList) embeddingField.getType()).getListSize()).isEqualTo(4);

        // Child should still be a float element
        assertThat(embeddingField.getChildren()).hasSize(1);
        assertThat(embeddingField.getChildren().get(0).getName()).isEqualTo("element");
    }

    @Test
    void testArrayColumnWithZeroSize() {
        RowType rowType =
                DataTypes.ROW(DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT())));

        Map<String, String> properties = new HashMap<>();
        properties.put("embedding.arrow.fixed-size-list.size", "0");

        assertThatThrownBy(() -> LanceArrowUtils.toArrowSchema(rowType, properties))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testArrayColumnWithNegativeSize() {
        RowType rowType =
                DataTypes.ROW(DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT())));

        Map<String, String> properties = new HashMap<>();
        properties.put("embedding.arrow.fixed-size-list.size", "-1");

        assertThatThrownBy(() -> LanceArrowUtils.toArrowSchema(rowType, properties))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testArrayColumnWithNonNumericSize() {
        RowType rowType =
                DataTypes.ROW(DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT())));

        Map<String, String> properties = new HashMap<>();
        properties.put("embedding.arrow.fixed-size-list.size", "abc");

        assertThatThrownBy(() -> LanceArrowUtils.toArrowSchema(rowType, properties))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testToArrowSchemaWithEmptyProperties() {
        RowType rowType =
                DataTypes.ROW(DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT())));

        Schema schema = LanceArrowUtils.toArrowSchema(rowType, Collections.emptyMap());

        Field embeddingField = schema.findField("embedding");
        assertThat(embeddingField.getType()).isInstanceOf(ArrowType.List.class);
    }

    @Test
    void testToArrowSchemaWithNullProperties() {
        RowType rowType =
                DataTypes.ROW(DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT())));

        Schema schema = LanceArrowUtils.toArrowSchema(rowType, null);

        Field embeddingField = schema.findField("embedding");
        assertThat(embeddingField.getType()).isInstanceOf(ArrowType.List.class);
    }
}
